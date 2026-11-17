//! 标准 SHA1、六位和三十秒周期的 TOTP 验证码

use gateway_admin::ports::provider::ProviderAdminError;
use hmac::{Hmac, KeyInit as _, Mac as _};
use sha1::Sha1;
use url::Url;

use super::invalid;

pub(super) fn generate(secret: &str, timestamp: i64) -> Result<String, ProviderAdminError> {
    let secret = if secret.starts_with("otpauth://") {
        let url = Url::parse(secret).map_err(|_| invalid("2FA 密钥地址不合法"))?;
        if url.host_str() != Some("totp")
            || url.query_pairs().any(|(key, value)| match key.as_ref() {
                "algorithm" => !value.eq_ignore_ascii_case("SHA1"),
                "digits" => value != "6",
                "period" => value != "30",
                _ => false,
            })
        {
            return Err(invalid("2FA 仅支持 SHA1、6 位验证码和 30 秒周期"));
        }
        url.query_pairs()
            .find(|(key, _)| key == "secret")
            .map(|(_, value)| value.into_owned())
            .ok_or_else(|| invalid("2FA 密钥格式不合法"))?
    } else {
        secret.to_owned()
    };
    let secret: String = secret
        .chars()
        .filter(|character| !character.is_ascii_whitespace() && *character != '=')
        .map(|character| character.to_ascii_uppercase())
        .collect();
    let bytes = data_encoding::BASE32_NOPAD
        .decode(secret.as_bytes())
        .map_err(|_| invalid("2FA 需填写 Base32 密钥或 otpauth 地址"))?;
    if bytes.is_empty() || bytes.len() > 128 || timestamp < 0 {
        return Err(invalid("2FA 密钥或时间不合法"));
    }
    let mut mac = Hmac::<Sha1>::new_from_slice(&bytes).map_err(|_| invalid("2FA 密钥不合法"))?;
    mac.update(&(timestamp as u64 / 30).to_be_bytes());
    let digest = mac.finalize().into_bytes();
    let offset = usize::from(digest[19] & 15);
    let value = u32::from_be_bytes([
        digest[offset] & 127,
        digest[offset + 1],
        digest[offset + 2],
        digest[offset + 3],
    ]);
    Ok(format!("{:06}", value % 1_000_000))
}
