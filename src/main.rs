use anyhow::{Context, Result};
use std::io::{self, BufWriter, Write};

use douyin_danmaku::browser_cookie;
use douyin_danmaku::client;
use douyin_danmaku::config;
use douyin_danmaku::sign;

/// 带缓冲的标准输出：高频弹幕下每行一次 println! 会带来锁竞争与大量系统调用，
/// 这里改为批量写入，每积累若干行或退出时统一 flush。
struct Out {
    w: BufWriter<io::Stdout>,
    pending: usize,
}

impl Out {
    fn new() -> Self {
        Out {
            w: BufWriter::with_capacity(64 * 1024, io::stdout()),
            pending: 0,
        }
    }

    fn line(&mut self, text: &str) {
        let _ = writeln!(self.w, "{}", text);
        self.pending += 1;
        if self.pending >= 32 {
            self.flush();
        }
    }

    fn flush(&mut self) {
        let _ = self.w.flush();
        self.pending = 0;
    }
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
        Some(arg) => {
            if arg.starts_with("http") {
                arg.trim_end_matches('/')
                    .rsplit('/')
                    .next()
                    .unwrap_or(&arg)
                    .split('?')
                    .next()
                    .unwrap_or("")
                    .to_string()
            } else {
                arg
            }
        }
        None => prompt(&mut out, "请输入抖音直播间 web_rid: ")?,
    };

    let use_edge_cookie = args.iter().any(|a| a == "--edge-cookie" || a == "--edge");

    out.line("=== 纯 Rust 抖音弹幕采集器 ===");
    out.line(&format!("直播间: {}", web_rid));
    if use_edge_cookie {
        out.line("身份: 已登录 (从 Edge 浏览器获取 Cookie)");
    } else {
        out.line("身份: 游客");
    }
    out.line("");

    let (ttwid, room_id) = sign::fetch_ttwid_and_room_id(&web_rid)
        .await
        .context("获取 ttwid 和 room_id 失败")?;
    out.line(&format!("  ttwid: {}...", &ttwid[..20.min(ttwid.len())]));
    out.line(&format!("  room_id: {}", room_id));

    let extra_cookies = if use_edge_cookie {
        out.line("");
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

    out.line("");
    out.line("生成签名...");
    let (ws_url, cookies) = sign::build_ws_url(&room_id, &ttwid, extra_cookies.as_deref())
        .await
        .context("生成签名失败")?;

    out.line("");
    out.line("连接 WebSocket...");
    out.line("");
    out.flush();

    let sign_config = config::WsSignConfig {
        ws_url,
        signature: String::new(),
        room_id: room_id.clone(),
        ttwid,
        ms_token: String::new(),
        cookies,
    };

    let danmaku_client = client::DanmakuClient::new();

    out.line("开始接收弹幕...");
    out.line("按 Ctrl+C 退出");
    out.line("");
    out.flush();

    let msg_count = danmaku_client
        .connect_with_sign(&sign_config, |msg| {
            out.line(&msg.to_string());
        })
        .await
        .context("WebSocket 连接失败")?;

    out.flush();
    out.line(&format!("共接收 {} 条消息", msg_count));

    Ok(())
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
