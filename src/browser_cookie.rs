use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use std::os::windows::ffi::OsStrExt;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::time::Duration;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message as WsMessage};

/// 从 Edge 浏览器的本地 Cookie 数据库读取抖音登录 Cookie（不影响用户正在使用的浏览器）
///
/// 流程：
/// 1. 复制用户的 Edge User Data 目录到临时目录（保留登录态）
/// 2. 启动 Edge headless + 复制的 User Data 目录 + 调试端口
/// 3. 通过 CDP 获取所有已解密的 cookie（Edge 自己读取的是明文）
/// 4. 关闭 Edge 进程
/// 5. 删除临时复制的配置目录
/// 6. 返回抖音相关 cookie
///
/// 优点：
/// - 不影响用户正在使用的 Edge（使用复制的配置）
/// - 正确处理 Chrome 127+ 的 App-Bound Encryption (v20)
/// - 纯 Rust 实现，无需 DLL 注入
pub async fn get_douyin_cookies_silent() -> Result<String> {
    println!("  正在静默获取 Edge Cookie（不影响您正在使用的浏览器）...");

    let user_profile = std::env::var("USERPROFILE")
        .unwrap_or_else(|_| r"C:\Users\Admin".to_string());

    let original_user_data = PathBuf::from(&user_profile)
        .join("AppData")
        .join("Local")
        .join("Microsoft")
        .join("Edge")
        .join("User Data");

    // 1. 创建临时目录存放复制的配置
    let temp_dir = std::env::temp_dir().join(format!("edge_cookie_copy_{}", std::process::id()));
    std::fs::create_dir_all(&temp_dir)
        .with_context(|| format!("创建临时目录失败: {:?}", temp_dir))?;

    let temp_user_data = temp_dir.join("User Data");

    // 2. 复制 User Data 目录（只复制必要文件以加快速度）
    println!("  正在复制 Edge 配置文件...");
    copy_edge_profile(&original_user_data, &temp_user_data)?;
    println!("  配置复制完成");

    // 3. 定位 Edge 可执行文件
    let edge_path = find_edge_path().context("未找到 Edge 可执行文件")?;

    // 4. 选择调试端口
    let port = 9223u16;

    // 5. 启动 Edge headless + 复制的 User Data 目录
    let mut edge_cmd = Command::new(&edge_path);
    edge_cmd.args([
        "--headless=new",
        "--disable-gpu",
        "--no-first-run",
        "--no-default-browser-check",
        "--disable-extensions",
        "--disable-sync",
        "--mute-audio",
        "--window-size=1,1",
        &format!("--remote-debugging-port={}", port),
        &format!("--user-data-dir={}", temp_user_data.display()),
        "--remote-allow-origins=*",
    ]);
    edge_cmd.stdout(Stdio::null()).stderr(Stdio::null());

    let mut edge_child = edge_cmd
        .spawn()
        .with_context(|| format!("启动 Edge 失败: {:?}", edge_path))?;

    println!("  Edge 已启动（使用复制的配置），等待 CDP 就绪...");

    // 6. 等待 Edge 的 CDP 端口就绪
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(3))
        .no_proxy()
        .build()?;

    let mut ready = false;
    for attempt in 0..40 {
        if let Ok(Some(status)) = edge_child.try_wait() {
            // 清理临时目录
            let _ = std::fs::remove_dir_all(&temp_dir);
            anyhow::bail!("Edge 进程过早退出 (status={})", status);
        }

        match client
            .get(format!("http://127.0.0.1:{}/json/version", port))
            .send()
            .await
        {
            Ok(resp) if resp.status().is_success() => {
                ready = true;
                break;
            }
            _ => {
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        }
        let _ = attempt;
    }

    if !ready {
        let _ = edge_child.kill();
        let _ = std::fs::remove_dir_all(&temp_dir);
        anyhow::bail!("Edge CDP 端口就绪超时（20 秒内未响应）");
    }

    // 7. 获取页面列表
    let pages: Vec<serde_json::Value> = client
        .get(format!("http://127.0.0.1:{}/json", port))
        .send()
        .await
        .context("获取 CDP 页面列表失败")?
        .json()
        .await
        .context("解析 CDP 页面列表失败")?;

    let ws_url = pages
        .iter()
        .find(|p| p.get("type").and_then(|v| v.as_str()) == Some("page"))
        .and_then(|p| p.get("webSocketDebuggerUrl").and_then(|v| v.as_str()))
        .context("未找到可调试的页面")?
        .to_string();

    // 8. WebSocket 连接
    let (ws_stream, _) = connect_async(&ws_url)
        .await
        .with_context(|| format!("WebSocket 连接 CDP 失败: {}", ws_url))?;
    let (mut ws_write, mut ws_read) = ws_stream.split();

    println!("  CDP 已连接，正在获取 Cookie...");

    // 9. 发送 Network.enable
    send_cdp_command(&mut ws_write, &mut ws_read, 1, "Network.enable", None).await?;

    // 10. 发送 Page.enable
    send_cdp_command(&mut ws_write, &mut ws_read, 2, "Page.enable", None).await?;

    // 11. 导航到抖音页面触发 cookie 加载
    let nav_params = serde_json::json!({"url": "https://live.douyin.com/"});
    send_cdp_command(&mut ws_write, &mut ws_read, 3, "Page.navigate", Some(&nav_params)).await?;

    // 12. 等待页面加载
    tokio::time::sleep(Duration::from_secs(4)).await;

    // 13. 获取 cookie
    let cookies_response = send_cdp_command(
        &mut ws_write,
        &mut ws_read,
        4,
        "Network.getAllCookies",
        None,
    )
    .await
    .context("获取 Network.getAllCookies 响应失败")?;

    let _ = ws_write.close().await;

    // 14. 关闭 Edge
    println!("  Cookie 获取完成，正在关闭 Edge...");
    let _ = edge_child.kill();
    let _ = edge_child.wait();

    // 15. 删除临时复制的配置目录
    let _ = std::fs::remove_dir_all(&temp_dir);

    // 16. 解析 cookie
    let mut all_cookies: Vec<(String, String, String)> = Vec::new();
    if let Some(cookies_array) = cookies_response
        .get("cookies")
        .and_then(|c| c.as_array())
    {
        for cookie in cookies_array {
            let name = cookie.get("name").and_then(|v| v.as_str()).unwrap_or("");
            let value = cookie.get("value").and_then(|v| v.as_str()).unwrap_or("");
            let domain = cookie.get("domain").and_then(|v| v.as_str()).unwrap_or("");
            all_cookies.push((name.to_string(), value.to_string(), domain.to_string()));
        }
    }

    // 17. 过滤抖音相关 cookie
    let mut douyin_cookies = Vec::new();
    for (name, value, domain) in &all_cookies {
        if domain.contains("douyin") || domain.contains("iesdouyin") {
            douyin_cookies.push(format!("{}={}", name, value));
        }
    }

    if douyin_cookies.is_empty() {
        anyhow::bail!("未获取到抖音相关 Cookie");
    }

    println!(
        "  成功获取 {} 个抖音 Cookie",
        douyin_cookies.len()
    );
    Ok(douyin_cookies.join("; "))
}

/// 复制 Edge 配置文件（只复制必要文件以加快速度）
fn copy_edge_profile(src: &PathBuf, dst: &PathBuf) -> Result<()> {
    // 需要复制的文件/目录
    let items_to_copy = [
        "Local State",
        "Default/Cookies",
        "Default/Network/Cookies",
        "Default/Session Storage",
        "Default/Local Storage",
        "Default/Login Data",
        "Default/Preferences",
    ];

    for item in items_to_copy {
        let src_item = src.join(item);
        let dst_item = dst.join(item);

        if src_item.exists() {
            let parent = dst_item.parent().unwrap();
            if !parent.exists() {
                std::fs::create_dir_all(parent)?;
            }

            if src_item.is_dir() {
                copy_dir_skip_locked(&src_item, &dst_item)?;
            } else {
                // 使用 Windows API 读取被锁定的文件
                if let Err(e) = copy_locked_file(&src_item, &dst_item) {
                    eprintln!("  [警告] 无法复制文件 {:?}: {}", src_item, e);
                    // 跳过锁定的文件，继续复制其他文件
                }
            }
        }
    }

    Ok(())
}

/// 递归复制目录（跳过锁定的文件）
fn copy_dir_skip_locked(src: &PathBuf, dst: &PathBuf) -> Result<()> {
    std::fs::create_dir_all(dst)?;

    for entry in std::fs::read_dir(src)? {
        let entry = entry?;
        let src_path = entry.path();
        let dst_path = dst.join(entry.file_name());

        if entry.file_type()?.is_dir() {
            let _ = copy_dir_skip_locked(&src_path, &dst_path);
        } else {
            let _ = copy_locked_file(&src_path, &dst_path);
        }
    }

    Ok(())
}

/// 使用 Windows API 复制被锁定的文件
fn copy_locked_file(src: &std::path::Path, dst: &std::path::Path) -> Result<()> {
    use windows::Win32::Foundation::{CloseHandle, INVALID_HANDLE_VALUE, GENERIC_READ, GENERIC_WRITE};
    use windows::Win32::Storage::FileSystem::{
        CreateFileW, ReadFile, WriteFile, FILE_SHARE_READ, FILE_SHARE_WRITE,
        FILE_SHARE_DELETE, OPEN_EXISTING, CREATE_ALWAYS, FILE_ATTRIBUTE_NORMAL,
    };

    let src_w: Vec<u16> = src.as_os_str().encode_wide().chain(std::iter::once(0)).collect();
    let dst_w: Vec<u16> = dst.as_os_str().encode_wide().chain(std::iter::once(0)).collect();

    let h_src = match unsafe {
        CreateFileW(
            windows::core::PCWSTR(src_w.as_ptr()),
            GENERIC_READ.0,
            FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
            None,
            OPEN_EXISTING,
            FILE_ATTRIBUTE_NORMAL,
            None,
        )
    } {
        Ok(h) => h,
        Err(_) => {
            // 如果 Windows API 也无法打开，尝试普通复制
            std::fs::copy(src, dst)?;
            return Ok(());
        }
    };

    if h_src.0 == INVALID_HANDLE_VALUE.0 {
        std::fs::copy(src, dst)?;
        return Ok(());
    }

    let h_dst = match unsafe {
        CreateFileW(
            windows::core::PCWSTR(dst_w.as_ptr()),
            GENERIC_WRITE.0,
            FILE_SHARE_READ,
            None,
            CREATE_ALWAYS,
            FILE_ATTRIBUTE_NORMAL,
            None,
        )
    } {
        Ok(h) => h,
        Err(_) => {
            let _ = unsafe { CloseHandle(h_src) };
            anyhow::bail!("CreateFileW 创建目标文件失败");
        }
    };

    if h_dst.0 == INVALID_HANDLE_VALUE.0 {
        let _ = unsafe { CloseHandle(h_src) };
        anyhow::bail!("CreateFileW 创建目标文件失败");
    }

    let mut buffer = vec![0u8; 65536];
    loop {
        let mut bytes_read = 0u32;
        let ok = unsafe {
            ReadFile(h_src, Some(&mut buffer), Some(&mut bytes_read), None)
        };

        if ok.is_err() {
            let _ = unsafe { CloseHandle(h_src) };
            let _ = unsafe { CloseHandle(h_dst) };
            anyhow::bail!("ReadFile 失败");
        }

        if bytes_read == 0 {
            break;
        }

        let mut bytes_written = 0u32;
        let ok = unsafe {
            WriteFile(h_dst, Some(&buffer[..bytes_read as usize]), Some(&mut bytes_written), None)
        };

        if ok.is_err() || bytes_written != bytes_read {
            let _ = unsafe { CloseHandle(h_src) };
            let _ = unsafe { CloseHandle(h_dst) };
            anyhow::bail!("WriteFile 失败");
        }
    }

    let _ = unsafe { CloseHandle(h_src) };
    let _ = unsafe { CloseHandle(h_dst) };
    Ok(())
}

/// 发送 CDP 命令并等待响应
async fn send_cdp_command(
    ws_write: &mut futures_util::stream::SplitSink<
        tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
        WsMessage,
    >,
    ws_read: &mut futures_util::stream::SplitStream<
        tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
    >,
    id: u64,
    method: &str,
    params: Option<&serde_json::Value>,
) -> Result<serde_json::Value> {
    let command = if let Some(p) = params {
        serde_json::json!({"id": id, "method": method, "params": p})
    } else {
        serde_json::json!({"id": id, "method": method})
    };
    ws_write
        .send(WsMessage::Text(command.to_string().into()))
        .await
        .with_context(|| format!("发送 CDP 命令失败: {}", method))?;

    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        tokio::select! {
            Some(msg) = ws_read.next() => {
                match msg {
                    Ok(WsMessage::Text(text)) => {
                        if let Ok(json) = serde_json::from_str::<serde_json::Value>(&text) {
                            if json.get("id") == Some(&serde_json::json!(id)) {
                                if let Some(error) = json.get("error") {
                                    anyhow::bail!("CDP 命令 {} 返回错误: {}", method, error);
                                }
                                return Ok(json
                                    .get("result")
                                    .cloned()
                                    .unwrap_or(serde_json::Value::Null));
                            }
                        }
                    }
                    Ok(WsMessage::Close(_)) => {
                        anyhow::bail!("WebSocket 连接已关闭");
                    }
                    Ok(_) => {}
                    Err(e) => {
                        anyhow::bail!("WebSocket 读取错误: {}", e);
                    }
                }
            }
            _ = tokio::time::sleep_until(deadline) => {
                anyhow::bail!("等待 CDP 命令 {} 响应超时", method);
            }
        }
    }
}

/// 定位 Edge 可执行文件路径
fn find_edge_path() -> Result<PathBuf> {
    let candidates = [
        r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe",
        r"C:\Program Files\Microsoft\Edge\Application\msedge.exe",
    ];
    for path in &candidates {
        if std::path::Path::new(path).exists() {
            return Ok(PathBuf::from(path));
        }
    }

    if let Ok(output) = Command::new("reg")
        .args([
            "query",
            r"HKLM\SOFTWARE\Microsoft\Windows\CurrentVersion\App Paths\msedge.exe",
            "/ve",
        ])
        .output()
    {
        let stdout = String::from_utf8_lossy(&output.stdout);
        for line in stdout.lines() {
            if let Some(idx) = line.find("REG_SZ") {
                let path = line[idx + 6..].trim().trim_matches('"');
                if std::path::Path::new(path).exists() {
                    return Ok(PathBuf::from(path));
                }
            }
        }
    }
    anyhow::bail!("未找到 Edge 可执行文件，请确认 Edge 已正确安装")
}
