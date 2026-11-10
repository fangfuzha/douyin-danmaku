use anyhow::{Context, Result};
use rand::RngExt;
use rquickjs::{Context as JsContext, Runtime};
use md5::{Md5, Digest};

const SIGN_JS: &str = include_str!("sign.js");

/// 用 rquickjs 执行抖音的 sign.js，生成 signature
pub fn generate_signature(md5_param: &str) -> Result<String> {
    let runtime = Runtime::new().context("Failed to create JS runtime")?;
    let ctx = JsContext::full(&runtime).context("Failed to create JS context")?;

    let result = ctx.with(|ctx| -> anyhow::Result<String> {
        // 先补环境：定义 sign.js 需要的全局变量
        let env_js = r#"
            var document = {};
            var window = {};
            var navigator = {userAgent: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'};
        "#;
        ctx.eval::<(), _>(env_js).map_err(|e| {
            anyhow::anyhow!("Failed to eval env.js: {:?}", e)
        })?;

        // 执行 sign.js（去掉前3行补环境代码，因为已经手动补了）
        let sign_js = SIGN_JS.lines().skip(3).collect::<Vec<_>>().join("\n");
        ctx.eval::<(), _>(sign_js.as_str()).map_err(|e| {
            anyhow::anyhow!("Failed to eval sign.js: {:?}", e)
        })?;

        // 调用 get_sign(md5_param)
        let global = ctx.globals();
        let func: rquickjs::Function = global
            .get("get_sign")
            .context("get_sign function not found")?;

        let signature: String = func
            .call((md5_param,))
            .map_err(|e| anyhow::anyhow!("Failed to call get_sign: {:?}", e))?;

        Ok(signature)
    })?;

    Ok(result)
}

/// 计算 MD5 hex（32位小写）
pub fn md5_hex(input: &str) -> String {
    let mut hasher = Md5::new();
    hasher.update(input.as_bytes());
    hasher
        .finalize()
        .iter()
        .map(|b| format!("{:02x}", b))
        .collect()
}

/// 生成 WebSocket URL（纯 Rust，无需浏览器）
/// extra_cookies: 额外的 cookie 字符串（如 sessionid 等），会与 ttwid 合并
pub async fn build_ws_url(room_id: &str, ttwid: &str, extra_cookies: Option<&str>) -> Result<(String, String)> {
    let did = generate_did();
    let timestamp = chrono::Utc::now().timestamp_millis();

    // 关键参数（与 Python 项目保持一致）
    let sdk_version = "1.0.14-beta.0";
    let browser_version = "5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36 Edg/140.0.0.0";

    // 签名参数（13个，顺序敏感）
    // Python: param = ','.join([f"{k}={wss_maps.get(k, '')}" for k in params])
    let sign_params = format!(
        "live_id=1,aid=6383,version_code=180800,webcast_sdk_version={},room_id={},sub_room_id=,sub_channel_id=,did_rule=3,user_unique_id={},device_platform=web,device_type=,ac=,identity=audience",
        sdk_version, room_id, did
    );
    let md5_str = md5_hex(&sign_params);

    // 用 sign.js 生成 signature
    let signature = generate_signature(&md5_str)?;
    println!("[签名] MD5={} -> signature={}", md5_str, signature);

    // 构建 internal_ext（与 Python 项目格式一致）
    let internal_ext = format!(
        "internal_src:dim|wss_push_room_id:{}|wss_push_did:{}|first_req_ms:{}|fetch_time:{}|seq:1|wss_info:0-{}-0-0|wrds_v:{}",
        room_id, did, timestamp, timestamp, timestamp, generate_random_id()
    );

    // cursor 格式参考 Python: d-1_u-1_fh-{random}_t-{timestamp}_r-1
    let cursor = format!("d-1_u-1_fh-{}_t-{}_r-1", generate_random_id(), timestamp);

    let params = [
        ("app_name", "douyin_web"),
        ("version_code", "180800"),
        ("webcast_sdk_version", sdk_version),
        ("update_version_code", sdk_version),
        ("compress", "gzip"),
        ("device_platform", "web"),
        ("cookie_enabled", "true"),
        ("screen_width", "1536"),
        ("screen_height", "864"),
        ("browser_language", "zh-CN"),
        ("browser_platform", "Win32"),
        ("browser_name", "Mozilla"),
        ("browser_version", browser_version),
        ("browser_online", "true"),
        ("tz_name", "Asia/Shanghai"),
        ("cursor", &cursor),
        ("internal_ext", &internal_ext),
        ("host", "https://live.douyin.com"),
        ("aid", "6383"),
        ("live_id", "1"),
        ("did_rule", "3"),
        ("endpoint", "live_pc"),
        ("support_wrds", "1"),
        ("user_unique_id", &did),
        ("im_path", "/webcast/im/fetch/"),
        ("identity", "audience"),
        ("need_persist_msg_count", "15"),
        ("insert_task_id", ""),
        ("live_reason", ""),
        ("room_id", room_id),
        ("heartbeatDuration", "0"),
        ("signature", &signature),
    ];

    let query = params
        .iter()
        .map(|(k, v)| format!("{}={}", k, urlencoding::encode(v)))
        .collect::<Vec<_>>()
        .join("&");

    let cookies = if let Some(extra) = extra_cookies {
        if extra.is_empty() {
            format!("ttwid={}", ttwid)
        } else {
            format!("ttwid={}; {}", ttwid, extra)
        }
    } else {
        format!("ttwid={}", ttwid)
    };

    // 使用 Python 项目相同的服务器域名
    let ws_url = format!(
        "wss://webcast100-ws-web-lq.douyin.com/webcast/im/push/v2/?{}",
        query
    );

    Ok((ws_url, cookies))
}

/// 通过 HTTP 请求获取 ttwid 和 room_id
pub async fn fetch_ttwid_and_room_id(web_rid: &str) -> Result<(String, String)> {
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .redirect(reqwest::redirect::Policy::limited(5))
        .build()?;

    let url = format!("https://live.douyin.com/{}", web_rid);
    let resp = client
        .get(&url)
        .header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36 Edg/140.0.0.0")
        .send()
        .await
        .context("Failed to fetch live page")?;

    // 从 Set-Cookie 提取 ttwid
    let mut ttwid = String::new();
    for cookie in resp.headers().get_all("set-cookie") {
        if let Ok(val) = cookie.to_str() {
            if val.starts_with("ttwid=") {
                ttwid = val
                    .split(';')
                    .next()
                    .unwrap_or("")
                    .trim_start_matches("ttwid=")
                    .to_string();
                break;
            }
        }
    }

    if ttwid.is_empty() {
        anyhow::bail!("未能从响应中获取 ttwid");
    }

    // 从响应体提取 room_id
    let body = resp.text().await.context("Failed to read response body")?;
    let room_id = extract_room_id(&body)?;

    Ok((ttwid, room_id))
}

/// 从直播间 HTML 中提取 roomId
fn extract_room_id(html: &str) -> Result<String> {
    // 尝试多种正则
    let patterns = [
        r#""roomId"\s*:\s*"(\d+)""#,
        r#"roomId\\?"\s*:\s*\\?"(\d+)\\?""#,
        r#"room_id\s*=\s*(\d+)"#,
    ];

    for pattern in &patterns {
        if let Ok(re) = regex::Regex::new(pattern) {
            if let Some(caps) = re.captures(html) {
                return Ok(caps[1].to_string());
            }
        }
    }

    anyhow::bail!("未能从 HTML 中提取 room_id")
}

/// 生成 19 位随机数字字符串作为 device id（模拟真实设备 ID 格式）
/// 参考 Python 项目中的 user_unique_id: 7319483754668557238
fn generate_did() -> String {
    let mut rng = rand::rng();
    // 第一位非0，共19位数字
    let first = rng.random_range(1..10u64);
    let rest: String = (0..18).map(|_| rng.random_range(0..10).to_string()).collect();
    format!("{}{}", first, rest)
}

fn generate_random_id() -> String {
    rand::rng()
        .sample_iter(rand::distr::Alphanumeric)
        .take(18)
        .map(char::from)
        .collect()
}

mod urlencoding {
    pub fn encode(s: &str) -> String {
        let mut result = String::new();
        for byte in s.as_bytes() {
            match byte {
                b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                    result.push(*byte as char);
                }
                _ => {
                    result.push_str(&format!("%{:02X}", byte));
                }
            }
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn md5_matches_known_vector() {
        assert_eq!(md5_hex("abc123"), "e99a18c428cb38d5f260853678922e03");
    }

    /// 冒烟测试：确认 rquickjs 升级后 sign.js 仍可正常执行并产出签名
    #[test]
    fn signature_is_produced_by_quickjs() {
        let signature = generate_signature("abc123").expect("签名生成失败");
        assert!(!signature.is_empty(), "签名不应为空");
    }
}
