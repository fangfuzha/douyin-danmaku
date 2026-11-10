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

/// 访问直播间页面时使用的请求头。
///
/// 实测：抖音会按请求头决定返回完整页面还是约 6KB 的空壳页，
/// 缺少 Accept / Accept-Language / sec-fetch-* 等头时几乎只会拿到空壳页。
fn page_headers(cookie: Option<&str>) -> reqwest::header::HeaderMap {
    let mut h = reqwest::header::HeaderMap::new();
    for (k, v) in [
        ("user-agent", USER_AGENT),
        ("accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8"),
        ("accept-language", "zh-CN,zh;q=0.9,en;q=0.8"),
        ("upgrade-insecure-requests", "1"),
        ("sec-ch-ua", "\"Chromium\";v=\"140\", \"Not=A?Brand\";v=\"24\", \"Microsoft Edge\";v=\"140\""),
        ("sec-ch-ua-mobile", "?0"),
        ("sec-ch-ua-platform", "\"Windows\""),
        ("sec-fetch-dest", "document"),
        ("sec-fetch-mode", "navigate"),
        ("sec-fetch-site", "none"),
        ("sec-fetch-user", "?1"),
    ] {
        let _ = cookie;
        h.insert(
            reqwest::header::HeaderName::from_bytes(k.as_bytes()).unwrap(),
            reqwest::header::HeaderValue::from_str(v).unwrap(),
        );
    }
    if let Some(c) = cookie {
        h.insert(
            reqwest::header::HeaderName::from_bytes(b"cookie").unwrap(),
            reqwest::header::HeaderValue::from_str(c).unwrap(),
        );
    }
    h
}

/// 获取 ttwid 与 room_id（同一个循环内完成，参考 BarrageGrab 的 GetTtwid / GetRoomId）。
///
/// 实测：抖音直播间页面存在「概率性返回空壳页」的现象——
/// 同一 URL 有时返回 1MB+ 的完整页面（含 roomId），有时只返回约 6KB 的空壳页（无 roomId）；
/// 同时响应头里的 ttwid 也只有约一半概率下发。
/// 因此这里统一成一个重试循环：每次请求都尝试收集 ttwid 与 roomId，直到两者都拿到（最多 8 次）。
pub async fn fetch_ttwid_and_room_id(web_rid: &str) -> Result<(String, String)> {
    fetch_room_info(web_rid, None).await
}

/// 同上，但允许传入已缓存的 ttwid。
///
/// 服务端并非每次响应都下发 ttwid（实测有的整轮 8 次都拿不到），
/// 因此 ttwid 一旦拿到就应当长期复用，只在彻底拿不到时才重新获取。
pub async fn fetch_room_info(web_rid: &str, cached_ttwid: Option<&str>) -> Result<(String, String)> {
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .redirect(reqwest::redirect::Policy::limited(5))

        .build()?;

    // (1) ttwid 只能来自 live.douyin.com 页面响应的 Set-Cookie。
    //     实测：抖音的 ttwid 注册接口（ttwid.bytedance.com）虽然稳定返回 ttwid，
    //     但那是 bytedance/ixigua 域的值，带到 live.douyin.com 会一直拿到空壳页。
    let mut ttwid = cached_ttwid.unwrap_or("").to_string();

    let url = format!("https://live.douyin.com/{}", web_rid);
    let mut room_id = String::new();
    let mut last_err = String::new();
    let mut net_err = String::new();
    let mut captcha = false;

    for attempt in 1..=8u32 {
        // 页面请求的 Cookie：只带 ttwid；还没拿到 ttwid 时干脆不带 Cookie。
        //
        // 实测（live.douyin.com）：
        // - 带 msToken 的组合（ttwid=..&msToken=..; __ac_nonce=..）几乎恒定返回
        //   约 6KB 的空壳页（拿不到 roomId）；
        // - 带任意 Cookie（含只有 __ac_nonce）时，服务端不再下发 ttwid；
        // - 只带 ttwid 时，约三分之一概率返回完整页面（1MB+，含 roomId）。
        let cookie = if ttwid.is_empty() {
            None
        } else {
            Some(format!("ttwid={}", ttwid))
        };

        match client
            .get(&url)
            .headers(page_headers(cookie.as_deref()))
            .send()
            .await
        {
            Ok(resp) => {
                let set_cookies: Vec<String> = resp
                    .headers()
                    .get_all("set-cookie")
                    .iter()
                    .filter_map(|v| v.to_str().ok().map(|s| s.to_string()))
                    .collect();
                let body = resp.text().await.unwrap_or_default();
                if body.contains("验证码") {
                    captcha = true;
                }

                if ttwid.is_empty() {
                    ttwid = extract_ttwid(&set_cookies)
                        .or_else(|| extract_ttwid_from_body(&body))
                        .unwrap_or_default();
                }
                if room_id.is_empty() {
                    if let Ok(id) = extract_room_id(&body) {
                        room_id = id;
                    }
                }
            }
            Err(e) => net_err = e.to_string(),
        }

        if !ttwid.is_empty() && !room_id.is_empty() {
            return Ok((ttwid, room_id));
        }

        last_err = if !net_err.is_empty() {
            format!("请求直播间页面失败：{}", net_err)
        } else if captcha {
            "触发了抖音的验证码风控（返回验证码中间页）".to_string()
        } else if ttwid.is_empty() {
            "响应中未包含 ttwid".to_string()
        } else {
            "页面未包含 roomId（服务端返回了空壳页）".to_string()
        };

        if attempt < 8 {
            // 触发风控时放慢节奏：连续快速请求只会让验证码一直出现
            let wait = if captcha { 5 } else { 2 };
            tokio::time::sleep(std::time::Duration::from_secs(wait)).await;
        }
    }

    anyhow::bail!("未能获取直播间信息（已重试 8 次）：{}", last_err)
}

/// ttwid 本地缓存文件路径
///
/// ttwid 有效期很长（服务端下发的是 Expires 一年后的 cookie），
/// 但每次访问直播间页面都可能触发抖音的验证码风控，
/// 因此把它缓存下来，跨次运行直接复用，避免重复触发风控。
fn ttwid_cache_path() -> Option<std::path::PathBuf> {
    let base = std::env::var_os("LOCALAPPDATA")
        .or_else(|| std::env::var_os("XDG_CACHE_HOME"))
        .or_else(|| std::env::var_os("HOME"))?;
    Some(std::path::PathBuf::from(base).join("douyin-danmaku").join("ttwid.txt"))
}

/// 读取本地缓存的 ttwid
pub fn load_cached_ttwid() -> Option<String> {
    let path = ttwid_cache_path()?;
    let content = std::fs::read_to_string(path).ok()?;
    let t = content.trim().to_string();
    if t.is_empty() {
        None
    } else {
        Some(t)
    }
}

/// 缓存 ttwid 到本地
pub fn save_cached_ttwid(ttwid: &str) {
    if let Some(path) = ttwid_cache_path() {
        if let Some(dir) = path.parent() {
            let _ = std::fs::create_dir_all(dir);
        }
        let _ = std::fs::write(path, ttwid);
    }
}

/// 删除本地缓存的 ttwid
pub fn clear_cached_ttwid() {
    if let Some(path) = ttwid_cache_path() {
        let _ = std::fs::remove_file(path);
    }
}

/// room_id 本地缓存文件路径（room_id 是场次标识，开播期间保持不变）
fn room_cache_path(web_rid: &str) -> Option<std::path::PathBuf> {
    let base = std::env::var_os("LOCALAPPDATA")
        .or_else(|| std::env::var_os("XDG_CACHE_HOME"))
        .or_else(|| std::env::var_os("HOME"))?;
    let safe: String = web_rid
        .chars()
        .filter(|c| c.is_ascii_alphanumeric())
        .collect();
    Some(
        std::path::PathBuf::from(base)
            .join("douyin-danmaku")
            .join(format!("room_{}.txt", safe)),
    )
}

/// 读取本地缓存的 room_id
pub fn load_cached_room(web_rid: &str) -> Option<String> {
    let path = room_cache_path(web_rid)?;
    let content = std::fs::read_to_string(path).ok()?;
    let r = content.trim().to_string();
    if r.is_empty() {
        None
    } else {
        Some(r)
    }
}

/// 缓存 room_id 到本地
pub fn save_cached_room(web_rid: &str, room_id: &str) {
    if let Some(path) = room_cache_path(web_rid) {
        if let Some(dir) = path.parent() {
            let _ = std::fs::create_dir_all(dir);
        }
        let _ = std::fs::write(path, room_id);
    }
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
