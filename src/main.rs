use anyhow::{Context, Result};
use std::io::{self, BufWriter, Write};
use std::time::{Duration, Instant};

use douyin_danmaku::browser_cookie;
use douyin_danmaku::client;
use douyin_danmaku::config;
use douyin_danmaku::models::MessageType;
use douyin_danmaku::sign;

/// 带缓冲的标准输出：高频弹幕下每行一次 println! 会带来锁竞争与大量系统调用，
/// 这里改为批量写入，每积累若干行或退出时统一 flush。
struct Out {
    w: BufWriter<io::Stdout>,
    pending: usize,
    last_flush: Instant,
}

impl Out {
    fn new() -> Self {
        Out {
            w: BufWriter::with_capacity(64 * 1024, io::stdout()),
            pending: 0,
            last_flush: Instant::now(),
        }
    }

    fn line(&mut self, text: &str) {
        let _ = writeln!(self.w, "{}", text);
        self.pending += 1;
        // 攒够 32 行或距上次刷新超过 300ms 就落盘：
        // 既保证弹幕高峰时的批量写入，也让偶发的系统消息能及时看到。
        if self.pending >= 32 || self.last_flush.elapsed() >= Duration::from_millis(300) {
            self.flush();
        }
    }

    fn flush(&mut self) {
        let _ = self.w.flush();
        self.pending = 0;
        self.last_flush = Instant::now();
    }
}

/// 断线重连的退避间隔：1s、2s、4s、8s、16s，之后封顶 30s。
///
/// 参考 jwwsjlm/douyinLive 的断线重连与 tiangalon/dy_danmaku_java 的重试间隔设计。
fn backoff_secs(attempt: u32) -> u64 {
    let exp = attempt.saturating_sub(1).min(5);
    (1u64 << exp).min(30)
}

#[tokio::main]
async fn main() -> Result<()> {
    let mut out = Out::new();

    let args: Vec<String> = std::env::args().collect();

    let room_arg = args
        .iter()
        .skip(1)
        .find(|a| !a.starts_with("--"))
        .cloned();

    let web_rid = match room_arg {
        Some(arg) => parse_web_rid(&arg),
        // 交互输入同样要支持直接粘贴分享链接
        None => parse_web_rid(&prompt(&mut out, "请输入抖音直播间 web_rid: ")?),
    };

    let use_edge_cookie = args.iter().any(|a| a == "--edge-cookie" || a == "--edge");
    // needAck 应答：服务端下发 needAck=1 后若不回应，会直接 RST 掉连接
    //（表现为 "Connection reset without closing handshake"），因此默认开启；
    // 仅在需要复现「不应答」行为时用 --no-ack 关闭。
    let enable_ack = !args.iter().any(|a| a == "--no-ack");
    // 未开播时的轮询间隔（秒）
    let poll_secs = flag_value(&args, "--poll-secs").unwrap_or(10);
    // 最大重连次数，0 表示无限重连
    let max_retries = flag_value(&args, "--max-retries").unwrap_or(0);
    // 是否打印签名等调试信息
    let verbose = args.iter().any(|a| a == "--verbose" || a == "-v");

    out.line("=== 纯 Rust 抖音弹幕采集器 ===");
    out.line(&format!("直播间: {}", web_rid));
    if use_edge_cookie {
        out.line("身份: 已登录 (从 Edge 浏览器获取 Cookie)");
    } else {
        out.line("身份: 游客");
    }
    out.line("");
    out.flush();

    let extra_cookies = if use_edge_cookie {
        out.line("正在从 Edge 浏览器静默获取登录 Cookie...");
        match browser_cookie::get_douyin_cookies_silent().await {
            Ok(cookies_str) => Some(cookies_str),
            Err(e) => {
                out.line(&format!("  获取失败: {}", e));
                out.line("  提示: 请确保在 Edge 浏览器中已登录抖音账号");
                out.flush();
                return Ok(());
            }
        }
    } else {
        None
    };

    let danmaku_client = client::DanmakuClient::new().with_ack(enable_ack);

    // 优先复用本地缓存的 ttwid / room_id：避免重复访问直播间页面触发抖音验证码风控
    let mut ttwid = sign::load_cached_ttwid().unwrap_or_default();
    let mut room_id = sign::load_cached_room(&web_rid).unwrap_or_default();
    let mut attempt: u32 = 0;

    // 所有输出统一经由回调，保证顺序
    let mut cb = |msg: MessageType| {
        out.line(&msg.to_string());
    };

    let msg_count = loop {
        // (1) 获取房间信息：拿不到 room_id 通常意味着还没开播，按间隔轮询
        if room_id.is_empty() {
            // ttwid 一旦拿到就长期复用（服务端并非每次都下发），
            // 这里把已缓存的 ttwid 传进去，只补齐缺失的 room_id。
            let cached = if ttwid.is_empty() {
                None
            } else {
                Some(ttwid.as_str())
            };
            match sign::fetch_room_info(&web_rid, cached).await {
                Ok((t, r)) => {
                    ttwid = t;
                    sign::save_cached_ttwid(&ttwid);
                    sign::save_cached_room(&web_rid, &r);
                    room_id = r;
                    cb(MessageType::System(format!(
                        "  ttwid: {}...",
                        &ttwid[..20.min(ttwid.len())]
                    )));
                    cb(MessageType::System(format!("  room_id: {}", room_id)));
                }
                Err(e) => {
                    cb(MessageType::System(format!(
                        "直播间未开播或信息获取失败：{}；{} 秒后重试",
                        e, poll_secs
                    )));
                    tokio::time::sleep(Duration::from_secs(poll_secs)).await;
                    continue;
                }
            }
        }

        // (2) 每次连接都重新签名（签名参数含时间戳，重连后必须刷新）
        let (ws_url, cookies) =
            match sign::build_ws_url(&room_id, &ttwid, extra_cookies.as_deref(), verbose).await {
                Ok(v) => v,
                Err(e) => {
                    cb(MessageType::System(format!("签名失败：{}；稍后重试", e)));
                    tokio::time::sleep(Duration::from_secs(backoff_secs(attempt.max(1)))).await;
                    attempt = attempt.saturating_add(1);
                    continue;
                }
            };

        let sign_config = config::WsSignConfig {
            ws_url,
            signature: String::new(),
            room_id: room_id.clone(),
            ttwid: ttwid.clone(),
            ms_token: String::new(),
            cookies,
        };

        // (3) 建立一次连接
        match danmaku_client.connect_once(&sign_config, &mut cb).await {
            Ok(n) => break n,
            Err(e) => {
                attempt = attempt.saturating_add(1);
                if max_retries > 0 && u64::from(attempt) >= max_retries {
                    cb(MessageType::System(format!(
                        "已达到最大重连次数（{}），退出",
                        max_retries
                    )));
                    break 0;
                }
                // 服务端明确说签名校验失败时，说明 ttwid 可能已失效，清掉缓存重来
                let err_text = e.to_string();
                if err_text.contains("not pass sign check") || err_text.contains("417") {
                    ttwid.clear();
                    sign::clear_cached_ttwid();
                }

                let delay = backoff_secs(attempt);
                cb(MessageType::System(format!(
                    "连接中断：{}；{} 秒后进行第 {} 次重连",
                    e, delay, attempt
                )));
                // 连续失败多次后重新拉取房间信息（room_id 是场次标识，可能已变）。
                // ttwid 不清空：它有效期很长，且服务端并非每次都下发，
                // 清空后很可能再也拿不到（实测有整轮 8 次都拿不到的情况）。
                if attempt % 3 == 0 {
                    room_id.clear();
                }
                tokio::time::sleep(Duration::from_secs(delay)).await;
            }
        }
    };

    out.flush();
    out.line(&format!("共接收 {} 条消息", msg_count));

    Ok(())
}

/// 从用户输入里提取直播间的 web_rid（短号）。
///
/// 支持三种写法：纯短号（587795807816）、直播间链接
/// （https://live.douyin.com/587795807816）、带查询串的分享链接
/// （https://live.douyin.com/587795807816?enter_from=...）。
fn parse_web_rid(input: &str) -> String {
    // 去掉首尾空白，以及管道/重定向输入可能带上的 UTF-8 BOM（U+FEFF）与零宽字符
    let s = input
        .trim()
        .trim_start_matches('\u{feff}')
        .trim_start_matches('\u{200b}')
        .trim();
    if !s.starts_with("http") {
        return s.to_string();
    }
    // 取路径最后一段，再去掉 ?query 与 #fragment
    let path = s
        .split(['?', '#'])
        .next()
        .unwrap_or("")
        .trim_end_matches('/');
    path.rsplit('/').next().unwrap_or("").to_string()
}

/// 读取形如 --flag 30 的命令行参数值
fn flag_value(args: &[String], name: &str) -> Option<u64> {
    let idx = args.iter().position(|a| a == name)?;
    args.get(idx + 1).and_then(|v| v.parse::<u64>().ok())
}

fn prompt(out: &mut Out, message: &str) -> Result<String> {
    let _ = write!(out.w, "{}", message);
    out.flush();

    let mut input = String::new();
    io::stdin()
        .read_line(&mut input)
        .context("Failed to read input")?;

    Ok(input.trim().to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_grows_then_caps() {
        assert_eq!(backoff_secs(1), 1);
        assert_eq!(backoff_secs(2), 2);
        assert_eq!(backoff_secs(3), 4);
        assert_eq!(backoff_secs(4), 8);
        assert_eq!(backoff_secs(5), 16);
        assert_eq!(backoff_secs(6), 30);
        assert_eq!(backoff_secs(100), 30);
    }

    #[test]
    fn parse_web_rid_accepts_plain_id_and_links() {
        assert_eq!(parse_web_rid("587795807816"), "587795807816");
        assert_eq!(
            parse_web_rid("https://live.douyin.com/587795807816"),
            "587795807816"
        );
        assert_eq!(
            parse_web_rid("https://live.douyin.com/587795807816?enter_from=x"),
            "587795807816"
        );
        assert_eq!(
            parse_web_rid("https://live.douyin.com/587795807816/"),
            "587795807816"
        );
        assert_eq!(parse_web_rid("  587795807816  "), "587795807816");
        // 管道输入可能带 UTF-8 BOM
        assert_eq!(
            parse_web_rid("\u{feff}https://live.douyin.com/587795807816"),
            "587795807816"
        );
    }

    #[test]
    fn flag_value_parses_next_argument() {
        let args = vec![
            "prog".to_string(),
            "123456".to_string(),
            "--poll-secs".to_string(),
            "30".to_string(),
        ];
        assert_eq!(flag_value(&args, "--poll-secs"), Some(30));
        assert_eq!(flag_value(&args, "--missing"), None);
    }
}
