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

/// 生成随机 token（msToken / __ac_nonce）。
///
/// 参考 BarrageGrab 的 GenerateMsToken：字符集为大小写字母 + 数字 + "=_"。
pub fn random_token(len: usize) -> String {
    const CHARSET: &[u8] = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789=_";
    let mut rng = rand::rng();
    (0..len)
        .map(|_| CHARSET[rng.random_range(0..CHARSET.len())] as char)
        .collect()
}

/// 生成 msToken（默认 107 位），参考 BarrageGrab
pub fn generate_ms_token() -> String {
    random_token(107)
}

/// 生成 __ac_nonce，参考 BarrageGrab：固定以 "0" 开头 + 20 位随机串
pub fn generate_ac_nonce() -> String {
    format!("0{}", random_token(20))
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
pub async fn build_ws_url(
    room_id: &str,
    ttwid: &str,
    extra_cookies: Option<&str>,
    verbose: bool,
) -> Result<(String, String)> {
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
    if verbose {
        println!("[签名] MD5={} -> signature={}", md5_str, signature);
    }

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

    // Cookie 只带 ttwid。
    //
    // 实测：握手带上 msToken/__ac_nonce 会被服务端拒绝（handshake-status: 417,
    // handshake-msg: "not pass sign check"），与参考项目 saermart/DouyinLiveWebFetcher
    // (liveMan.py) 及 BarrageGrab (cookie: ttwid={Ttwid}) 一致：
    // msToken/__ac_nonce 只用于页面请求与 enter API，不用于 WebSocket 握手。
    let mut cookies = format!("ttwid={}", ttwid);
    if let Some(extra) = extra_cookies {
        if !extra.is_empty() {
            cookies.push_str("; ");
            cookies.push_str(extra);
        }
    }

    // 使用 Python 项目相同的服务器域名
    let ws_url = format!(
        "wss://webcast100-ws-web-lq.douyin.com/webcast/im/push/v2/?{}",
        query
    );

    Ok((ws_url, cookies))
}

const USER_AGENT: &str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36 Edg/140.0.0.0";

/// 获取 ttwid 与 room_id。
///
/// 分两步，与参考项目 BarrageGrab 的 GetTtwid / GetRoomId 保持一致：
/// 1. 先请求直播间页面拿 ttwid（服务端并非每次响应都下发 ttwid，需要重试）；
/// 2. 再带着 ttwid + msToken + __ac_nonce 请求页面，从 HTML 中提取 roomId。
pub async fn fetch_ttwid_and_room_id(web_rid: &str) -> Result<(String, String)> {
    let ttwid = fetch_ttwid(web_rid).await?;
    let room_id = fetch_room_id(web_rid, &ttwid).await?;
    Ok((ttwid, room_id))
}

/// 请求直播间页面，返回 (ttwid, 响应体)
async fn request_live_page(web_rid: &str, cookie: Option<&str>) -> Result<(String, String)> {
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .redirect(reqwest::redirect::Policy::limited(5))
        .build()?;

    let url = format!("https://live.douyin.com/{}", web_rid);
    let mut req = client.get(&url).header("User-Agent", USER_AGENT);
    if let Some(c) = cookie {
        req = req.header("cookie", c);
    }

    let resp = req.send().await.context("Failed to fetch live page")?;
    if !resp.status().is_success() {
        anyhow::bail!("直播间页面返回 HTTP {}", resp.status());
    }

    // 优先从 Set-Cookie 头提取 ttwid
    let set_cookies: Vec<String> = resp
        .headers()
        .get_all("set-cookie")
        .iter()
        .filter_map(|v| v.to_str().ok().map(|s| s.to_string()))
        .collect();
    let body = resp.text().await.context("Failed to read response body")?;

    let ttwid = extract_ttwid(&set_cookies)
        .or_else(|| extract_ttwid_from_body(&body))
        .unwrap_or_default();

    Ok((ttwid, body))
}

/// 取 ttwid，最多尝试 5 次。
///
/// 参考 BarrageGrab 的 GetTtwid：服务端并非每次都下发 ttwid 头，
/// 失败就重来，最多 5 次。
async fn fetch_ttwid(web_rid: &str) -> Result<String> {
    // 访问直播间页面时带上 __ac_nonce（参考 BarrageGrab）
    let cookie = format!("__ac_nonce={}", generate_ac_nonce());
    let mut last_err = String::new();

    for attempt in 1..=5u32 {
        match request_live_page(web_rid, Some(&cookie)).await {
            Ok((ttwid, _)) if !ttwid.is_empty() => return Ok(ttwid),
            Ok(_) => last_err = "响应中未包含 ttwid".to_string(),
            Err(e) => last_err = e.to_string(),
        }
        if attempt < 5 {
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        }
    }

    anyhow::bail!("未能从响应中获取 ttwid（已重试 5 次）：{}", last_err)
}

/// 带 ttwid 请求页面并提取 roomId
async fn fetch_room_id(web_rid: &str, ttwid: &str) -> Result<String> {
    // 页面请求的 Cookie 与 BarrageGrab 一致：ttwid + msToken; __ac_nonce
    let cookie = format!(
        "ttwid={}&msToken={}; __ac_nonce={}",
        ttwid,
        generate_ms_token(),
        generate_ac_nonce()
    );

    let (_, body) = request_live_page(web_rid, Some(&cookie)).await?;
    extract_room_id(&body)
}

/// 从 Set-Cookie 头列表中提取 ttwid 值
fn extract_ttwid(set_cookies: &[String]) -> Option<String> {
    for raw in set_cookies {
        let val = raw.trim();
        if let Some(rest) = val.strip_prefix("ttwid=") {
            let v = rest.split(';').next().unwrap_or("").trim();
            if !v.is_empty() {
                return Some(v.to_string());
            }
        }
    }
    None
}

/// 兜底：部分情况下 ttwid 会被直接写进页面 HTML
fn extract_ttwid_from_body(html: &str) -> Option<String> {
    let re = regex::Regex::new(r#""ttwid"\s*:\s*"([^"]+)""#).ok()?;
    re.captures(html).map(|c| c[1].to_string())
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

    #[test]
    fn ttwid_is_extracted_from_set_cookie() {
        let cookies = vec![
            "ttwid=1%7Cabc==; Max-Age=604800; Path=/".to_string(),
            "other=1".to_string(),
        ];
        assert_eq!(extract_ttwid(&cookies).as_deref(), Some("1%7Cabc=="));
        assert!(extract_ttwid(&["other=1".to_string()]).is_none());
    }

    #[test]
    fn ttwid_is_extracted_from_body_as_fallback() {
        let html = r#"{"ttwid":"xyz789","other":1}"#;
        assert_eq!(extract_ttwid_from_body(html).as_deref(), Some("xyz789"));
    }

    /// 冒烟测试：确认 rquickjs 升级后 sign.js 仍可正常执行并产出签名
    #[test]
    fn signature_is_produced_by_quickjs() {
        let signature = generate_signature("abc123").expect("签名生成失败");
        assert!(!signature.is_empty(), "签名不应为空");
    }
}
