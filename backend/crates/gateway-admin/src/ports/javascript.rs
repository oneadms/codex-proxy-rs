//! Provider 所需的受管 JavaScript 进程与有界消息合同

use async_trait::async_trait;
use secrecy::SecretString;

/// 运行时程序及 SDK 输入输出共用有界通道，消息内容不得进入普通日志
pub const MAX_JAVASCRIPT_MESSAGE_BYTES: usize = 16 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum JavaScriptError {
    #[error("JavaScript runtime unavailable")]
    Unavailable,
    #[error("JavaScript runtime capacity exhausted")]
    Capacity,
    #[error("JavaScript runtime communication failed")]
    Communication,
    #[error("JavaScript runtime message limit exceeded")]
    MessageLimit,
    #[error("JavaScript runtime exited")]
    Closed,
}

#[async_trait]
pub trait JavaScriptRuntime: Send + Sync {
    /// 程序由 Provider 提供，Host 仅拥有进程、环境与通信生命周期
    async fn start(
        &self,
        program: &str,
        timezone: &str,
    ) -> Result<Box<dyn JavaScriptSession>, JavaScriptError>;
}

#[async_trait]
pub trait JavaScriptSession: Send {
    async fn send(&mut self, message: &SecretString) -> Result<(), JavaScriptError>;
    async fn receive(&mut self) -> Result<SecretString, JavaScriptError>;
    async fn close(self: Box<Self>);
}
