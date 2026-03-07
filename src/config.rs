#[cfg(feature = "lighter-sdk")]
use debot_utils::decrypt_data_with_kms;
use rust_decimal::Error as DecimalParseError;
use std::env;
use std::fmt;
use std::num::{ParseFloatError, ParseIntError};

#[cfg(feature = "lighter-sdk")]
#[derive(Debug)]
pub struct LighterConfig {
    pub api_key: String,
    pub private_key: String,
    pub evm_wallet_private_key: Option<String>,
    pub api_key_index: u32,
    pub account_index: u64,
    pub base_url: String,
    pub websocket_url: String,
}

#[derive(Debug)]
pub enum ConfigError {
    ParseIntError(ParseIntError),
    ParseFloatError(ParseFloatError),
    DecimalParseError(DecimalParseError),
    #[cfg(feature = "lighter-sdk")]
    OtherError(String),
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ConfigError::ParseIntError(e) => write!(f, "Parse int error: {}", e),
            ConfigError::ParseFloatError(e) => write!(f, "Parse float error: {}", e),
            ConfigError::DecimalParseError(e) => write!(f, "Decimal parse error: {}", e),
            #[cfg(feature = "lighter-sdk")]
            ConfigError::OtherError(e) => write!(f, "Other error: {}", e),
        }
    }
}

impl From<ParseIntError> for ConfigError {
    fn from(err: ParseIntError) -> ConfigError {
        ConfigError::ParseIntError(err)
    }
}

impl From<ParseFloatError> for ConfigError {
    fn from(err: ParseFloatError) -> ConfigError {
        ConfigError::ParseFloatError(err)
    }
}

impl From<rust_decimal::Error> for ConfigError {
    fn from(err: rust_decimal::Error) -> ConfigError {
        ConfigError::DecimalParseError(err)
    }
}

#[cfg(feature = "lighter-sdk")]
pub async fn get_lighter_config_from_env(prefix: &str) -> Result<LighterConfig, ConfigError> {
    // prefix is "" for main account, "HEDGE_" for hedge account
    let plain_priv_key = format!("LIGHTER_{}PLAIN_PRIVATE_API_KEY", prefix);
    let plain_pub_key = format!("LIGHTER_{}PLAIN_PUBLIC_API_KEY", prefix);
    let enc_priv_key = format!("LIGHTER_{}PRIVATE_API_KEY", prefix);
    let enc_pub_key = format!("LIGHTER_{}PUBLIC_API_KEY", prefix);
    let evm_key_name = format!("LIGHTER_{}EVM_WALLET_PRIVATE_KEY", prefix);
    let idx_key_name = format!("LIGHTER_{}API_KEY_INDEX", prefix);
    let acc_key_name = format!("LIGHTER_{}ACCOUNT_INDEX", prefix);

    let plain_private_api_key = env::var(&plain_priv_key).ok();
    let plain_public_api_key = env::var(&plain_pub_key).ok();
    let private_api_key = env::var(&enc_priv_key).ok();
    let public_api_key = env::var(&enc_pub_key).ok();
    let evm_wallet_private_key = env::var(&evm_key_name).ok();

    let (api_key, private_key, evm_wallet_key) = if let (Some(plain_priv), Some(plain_pub)) =
        (plain_private_api_key, plain_public_api_key)
    {
        log::info!(
            "Using plain text keys for {} account",
            if prefix.is_empty() { "main" } else { "hedge" }
        );
        let evm_wallet_key = if let Some(evm_key) = evm_wallet_private_key {
            let encrypted_data_key = env::var("ENCRYPTED_DATA_KEY")
                .expect("ENCRYPTED_DATA_KEY must be set")
                .replace(" ", "");
            let evm_key_vec = decrypt_data_with_kms(&encrypted_data_key, evm_key, true)
                .await
                .map_err(|_| {
                    ConfigError::OtherError("decrypt evm_wallet_private_key".to_owned())
                })?;
            Some(String::from_utf8(evm_key_vec).unwrap())
        } else {
            None
        };
        (plain_pub, plain_priv, evm_wallet_key)
    } else {
        log::info!(
            "Using KMS encrypted keys for {} account",
            if prefix.is_empty() { "main" } else { "hedge" }
        );
        let api_key = public_api_key.expect(&format!("{} must be set", enc_pub_key));
        let private_key = private_api_key.expect(&format!("{} must be set", enc_priv_key));
        let encrypted_data_key = env::var("ENCRYPTED_DATA_KEY")
            .expect("ENCRYPTED_DATA_KEY must be set")
            .replace(" ", "");
        let api_key_vec = decrypt_data_with_kms(&encrypted_data_key, api_key, true)
            .await
            .map_err(|_| ConfigError::OtherError("decrypt api_key".to_owned()))?;
        let api_key = String::from_utf8(api_key_vec).unwrap();
        let private_key_vec = decrypt_data_with_kms(&encrypted_data_key, private_key, true)
            .await
            .map_err(|_| ConfigError::OtherError("decrypt private_key".to_owned()))?;
        let private_key = String::from_utf8(private_key_vec).unwrap();
        let evm_wallet_key = if let Some(evm_key) = evm_wallet_private_key {
            let evm_key_vec = decrypt_data_with_kms(&encrypted_data_key, evm_key, true)
                .await
                .map_err(|_| {
                    ConfigError::OtherError("decrypt evm_wallet_private_key".to_owned())
                })?;
            Some(String::from_utf8(evm_key_vec).unwrap())
        } else {
            None
        };
        (api_key, private_key, evm_wallet_key)
    };

    let base_url = env::var("REST_ENDPOINT")
        .unwrap_or_else(|_| "https://mainnet.zklighter.elliot.ai/".to_string());
    let websocket_url = env::var("WEB_SOCKET_ENDPOINT")
        .unwrap_or_else(|_| "wss://mainnet.zklighter.elliot.ai/stream".to_string());

    let api_key_index: u32 = env::var(&idx_key_name)
        .unwrap_or_else(|_| "0".to_string())
        .parse()
        .expect(&format!("{} must be a valid u32", idx_key_name));
    let account_index: u64 = env::var(&acc_key_name)
        .unwrap_or_else(|_| "0".to_string())
        .parse()
        .expect(&format!("{} must be a valid u64", acc_key_name));

    Ok(LighterConfig {
        api_key,
        private_key,
        evm_wallet_private_key: evm_wallet_key,
        api_key_index,
        account_index,
        base_url,
        websocket_url,
    })
}
