use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::fs;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Config {
    pub cookie: CookieConfig,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CookieConfig {
    pub ttwid: String,
    pub sessionid: Option<String>,
    pub sessionid_ss: Option<String>,
}

/// 从 Python 脚本生成的 ws_sign.json 读取的签名配置
/// 用于 Rust 客户端直接使用真实签名连接 WebSocket
#[derive(Debug, Deserialize, Clone)]
pub struct WsSignConfig {
    pub ws_url: String,
    pub signature: String,
    pub room_id: String,
    pub ttwid: String,
    #[serde(default)]
    pub ms_token: String,
    pub cookies: String,
}

impl WsSignConfig {
    /// 从 ws_sign.json 加载签名配置
    /// 依次查找: 当前目录、上级目录（Python 脚本所在目录）
    pub fn load() -> Result<Self> {
        let candidates = [
            std::path::PathBuf::from("ws_sign.json"),
            std::path::PathBuf::from("../ws_sign.json"),
        ];
        for path in &candidates {
            if path.exists() {
                let content = fs::read_to_string(path)
                    .with_context(|| format!("Failed to read ws_sign.json: {:?}", path))?;
                return serde_json::from_str(&content)
                    .with_context(|| "Failed to parse ws_sign.json");
            }
        }
        anyhow::bail!(
            "未找到 ws_sign.json，请先在项目根目录运行: python danmu_ws.py --sign-only"
        )
    }
}

impl Config {
    pub fn load() -> Result<Self> {
        let path = Self::config_path()?;
        if !path.exists() {
            return Ok(Self::default());
        }

        let content = fs::read_to_string(&path)
            .with_context(|| format!("Failed to read config file: {:?}", path))?;

        serde_json::from_str(&content)
            .with_context(|| format!("Failed to parse config file: {:?}", path))
    }

    pub fn save(&self) -> Result<()> {
        let path = Self::config_path()?;

        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("Failed to create config directory: {:?}", parent))?;
        }

        let content = serde_json::to_string_pretty(self)
            .with_context(|| "Failed to serialize config")?;

        fs::write(&path, content)
            .with_context(|| format!("Failed to write config file: {:?}", path))
    }

    pub fn has_valid_cookie(&self) -> bool {
        !self.cookie.ttwid.is_empty()
    }

    fn config_path() -> Result<std::path::PathBuf> {
        Ok(std::path::PathBuf::from("config.json"))
    }
}

impl Default for Config {
    fn default() -> Self {
        Config {
            cookie: CookieConfig {
                ttwid: String::new(),
                sessionid: None,
                sessionid_ss: None,
            },
        }
    }
}

pub fn get_cookie_string(config: &Config) -> String {
    let mut parts = Vec::new();

    if !config.cookie.ttwid.is_empty() {
        parts.push(format!("ttwid={}", config.cookie.ttwid));
    }

    if let Some(sessionid) = &config.cookie.sessionid {
        parts.push(format!("sessionid={}", sessionid));
    }

    if let Some(sessionid_ss) = &config.cookie.sessionid_ss {
        parts.push(format!("sessionid_ss={}", sessionid_ss));
    }

    parts.join("; ")
}