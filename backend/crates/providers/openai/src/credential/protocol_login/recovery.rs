//! 根据上游认证失败事实执行协议重登，复用刷新容量、账号互斥与凭据版本保护

use std::{
    sync::Arc,
    time::{Duration, SystemTime},
};

use futures::{StreamExt as _, stream};
use gateway_admin::ports::{javascript::JavaScriptRuntime, provider::ProviderAdminError};
use gateway_core::{
    account::{AccountErrorReason, CredentialState, ProviderAccount, ProviderAccountId},
    diagnostics::{OperationalDiagnostics, OperationalFailure},
    provider_ports::{
        ProviderLeaseAcquisition, ProviderLeasePort, ProviderLeaseRequest,
        ProviderRefreshCapacityRequest, ProviderRefreshLeaseRequest, ProviderRuntimePolicyPort,
    },
};
use secrecy::SecretString;
use serde_json::json;

use crate::credential::{
    CodexCredentialCodec, CodexCredentialRepository, CodexOAuthSecret, CredentialRepositoryError,
};

#[derive(Debug, thiserror::Error)]
pub(crate) enum ProtocolLoginRecoveryError {
    #[error(transparent)]
    Repository(#[from] CredentialRepositoryError),
    #[error(transparent)]
    Store(#[from] gateway_core::provider_ports::ProviderStoreError),
}

pub(crate) struct CodexProtocolLoginRecovery {
    repository: CodexCredentialRepository,
    leases: Arc<dyn ProviderLeasePort>,
    runtime_policy: Arc<dyn ProviderRuntimePolicyPort>,
    javascript: Arc<dyn JavaScriptRuntime>,
    diagnostics: Arc<dyn OperationalDiagnostics>,
}

impl CodexProtocolLoginRecovery {
    pub(crate) fn new(
        repository: CodexCredentialRepository,
        leases: Arc<dyn ProviderLeasePort>,
        runtime_policy: Arc<dyn ProviderRuntimePolicyPort>,
        javascript: Arc<dyn JavaScriptRuntime>,
        diagnostics: Arc<dyn OperationalDiagnostics>,
    ) -> Self {
        Self {
            repository,
            leases,
            runtime_policy,
            javascript,
            diagnostics,
        }
    }

    pub(crate) async fn recover_due(
        &self,
    ) -> Result<Vec<ProviderAccountId>, ProtocolLoginRecoveryError> {
        let policy = self.runtime_policy.load_refresh_policy().await?;
        let candidates = self.repository.list_for_provider().await?;
        let results = stream::iter(
            candidates
                .into_iter()
                .filter(needs_relogin)
                .map(
                    |account| async move { self.recover_one(account, policy.concurrency()).await },
                ),
        )
        .buffer_unordered(policy.concurrency().get() as usize)
        .collect::<Vec<_>>()
        .await;
        results.into_iter().filter_map(Result::transpose).collect()
    }

    async fn recover_one(
        &self,
        account: ProviderAccount,
        concurrency: std::num::NonZeroU32,
    ) -> Result<Option<ProviderAccountId>, ProtocolLoginRecoveryError> {
        let loaded = self
            .repository
            .store()
            .load_current_credential(account.id())
            .await
            .map_err(CredentialRepositoryError::from)?;
        if loaded.account.revision() != account.revision() || !needs_relogin(&loaded.account) {
            return Ok(None);
        }
        let account = loaded.account.clone();
        let data = CodexCredentialCodec::decode_complete(&loaded.credential)
            .map_err(CredentialRepositoryError::from)?;
        let Some(credentials) = data.oauth().and_then(|oauth| oauth.protocol_login.as_ref()) else {
            return Ok(None);
        };
        let _capacity = match self
            .leases
            .try_acquire(ProviderLeaseRequest::RefreshCapacity(
                ProviderRefreshCapacityRequest::new(concurrency),
            ))
            .await?
        {
            ProviderLeaseAcquisition::Acquired(guard) => guard,
            ProviderLeaseAcquisition::Busy { .. } => return Ok(None),
        };
        let _guard = match self
            .leases
            .try_acquire(ProviderLeaseRequest::Refresh(
                ProviderRefreshLeaseRequest::new(account.id().clone(), account.revision()),
            ))
            .await?
        {
            ProviderLeaseAcquisition::Acquired(guard) => guard,
            ProviderLeaseAcquisition::Busy { .. } => return Ok(None),
        };
        // 等待租约期间可能已人工授权或恢复，发起登录前再次核对完整账号事实
        let current = self
            .repository
            .store()
            .load_current_credential(account.id())
            .await
            .map_err(CredentialRepositoryError::from)?;
        if current.account != loaded.account {
            return Ok(None);
        }
        let login = super::prepare_protocol_login(
            json!({
                "authentication_kind": "protocol_login",
                "username": credentials.username,
                "password": credentials.password,
                "totpSecret": credentials.totp_secret,
            }),
            account.outbound_proxy(),
            account.request_location(),
            Arc::clone(&self.javascript),
            self.repository.store().as_ref(),
        );
        // 刷新租约为五分钟，留出关闭浏览器和提交凭据的时间
        let result = tokio::time::timeout(Duration::from_secs(240), login)
            .await
            .map_err(|_| super::invalid("自动协议重登超时，请重新导入账号"))
            .and_then(|result| result)
            .and_then(|login| {
                let access_token =
                    SecretString::from(super::text(&login.tokens, "accessToken")?.to_owned());
                let id_token = super::text(&login.tokens, "idToken")?;
                let metadata = crate::credential::types::parse_chatgpt_jwt_claims(id_token)
                    .map_err(|_| super::invalid("自动协议重登返回的身份不完整"))?
                    .with_access_token_fallback(&access_token);
                if metadata.chatgpt_user_id.as_deref() != account.upstream_user_id()
                    || metadata.chatgpt_account_id.as_deref() != account.upstream_account_id()
                {
                    return Err(super::invalid("自动协议重登返回的账号或工作区不一致"));
                }
                Ok((
                    CodexOAuthSecret {
                        access_token,
                        refresh_token: Some(SecretString::from(
                            super::text(&login.tokens, "refreshToken")?.to_owned(),
                        )),
                        id_token: Some(SecretString::from(id_token.to_owned())),
                    },
                    login.device,
                ))
            });
        let (secret, device) = match result {
            Ok(login) => login,
            Err(error) => return self.record_failure(&account, error).await,
        };
        match self
            .repository
            .rotate_protocol_login(&account, secret, device)
            .await
        {
            Ok(_) => {
                tracing::info!(account_id = %account.id(), "OpenAI protocol login recovered");
                Ok(Some(account.id().clone()))
            }
            Err(CredentialRepositoryError::RevisionConflict(_)) => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    async fn record_failure(
        &self,
        account: &ProviderAccount,
        error: ProviderAdminError,
    ) -> Result<Option<ProviderAccountId>, ProtocolLoginRecoveryError> {
        let message = error
            .public_message()
            .unwrap_or("自动协议重登失败，请重新导入账号");
        let mut failure =
            OperationalFailure::new("oauth", "protocol_relogin", "login_failed", message);
        failure.provider_kind = Some(account.provider().clone());
        failure.account_id = Some(account.id().clone());
        failure.account_snapshot = Some(
            gateway_core::account::AccountHistorySnapshot::from_account(account),
        );
        failure.details = gateway_core::error::ErrorDetails::capture(
            Some(&gateway_core::error::ErrorSource::new(error)),
            None,
            false,
        );
        if self.diagnostics.record_failure(failure).await.is_err() {
            tracing::warn!(account_id = %account.id(), "Protocol login diagnostic could not be recorded");
        }
        // 失败后停止自动重放，人工验证或结果未知都需要管理员重新导入
        match self
            .repository
            .apply_state_with_reason(
                account,
                CredentialState::Expired,
                SystemTime::now(),
                Some(AccountErrorReason::CredentialExpired),
                Some(message.to_owned()),
            )
            .await
        {
            Ok(()) => Ok(Some(account.id().clone())),
            Err(CredentialRepositoryError::RevisionConflict(_)) => Ok(None),
            Err(error) => Err(error.into()),
        }
    }
}

fn needs_relogin(account: &ProviderAccount) -> bool {
    account.authentication_kind() == super::super::CODEX_AUTHENTICATION_KIND_OAUTH
        && account.credential_state() == CredentialState::Expired
        && account.last_error_reason() == Some(AccountErrorReason::AccessTokenExpired)
}
