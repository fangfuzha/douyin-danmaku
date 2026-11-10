use anyhow::{Context, Result};
use std::io::{self, Write};

use douyin_danmaku::browser_cookie;
use douyin_danmaku::client;
use douyin_danmaku::config;
use douyin_danmaku::sign;

#[tokio::main]
async fn main() -> Result<()> {
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
        None => prompt("请输入抖音直播间 web_rid: ")?,
    };

    let use_edge_cookie = args.iter().any(|a| a == "--edge-cookie" || a == "--edge");

    println!("=== 纯 Rust 抖音弹幕采集器 ===");
    println!("直播间: {}", web_rid);
    if use_edge_cookie {
        println!("身份: 已登录 (从 Edge 浏览器获取 Cookie)");
    } else {
        println!("身份: 游客");
    }
    println!();

    let (ttwid, room_id) = sign::fetch_ttwid_and_room_id(&web_rid)
        .await
        .context("获取 ttwid 和 room_id 失败")?;
    println!("  ttwid: {}...", &ttwid[..20.min(ttwid.len())]);
    println!("  room_id: {}", room_id);

    let extra_cookies = if use_edge_cookie {
        println!();
        println!("正在从 Edge 浏览器静默获取登录 Cookie...");
        match browser_cookie::get_douyin_cookies_silent().await {
            Ok(cookies_str) => Some(cookies_str),
            Err(e) => {
                println!("  获取失败: {}", e);
                println!("  提示: 请确保在 Edge 浏览器中已登录抖音账号");
                return Ok(());
            }
        }
    } else {
        None
    };

    println!();
    println!("生成签名...");
    let (ws_url, cookies) = sign::build_ws_url(&room_id, &ttwid, extra_cookies.as_deref())
        .await
        .context("生成签名失败")?;

    println!();
    println!("连接 WebSocket...");
    println!();

    let sign_config = config::WsSignConfig {
        ws_url,
        signature: String::new(),
        room_id: room_id.clone(),
        ttwid,
        ms_token: String::new(),
        cookies,
    };

    let danmaku_client = client::DanmakuClient::new();

    println!("开始接收弹幕...");
    println!("按 Ctrl+C 退出");
    println!();

    danmaku_client
        .connect_with_sign(&sign_config, |msg| {
            println!("{}", msg);
        })
        .await
        .context("WebSocket 连接失败")?;

    Ok(())
}

fn prompt(message: &str) -> Result<String> {
    print!("{}", message);
    io::stdout().flush().context("Failed to flush stdout")?;

    let mut input = String::new();
    io::stdin()
        .read_line(&mut input)
        .context("Failed to read input")?;

    Ok(input.trim().to_string())
}
