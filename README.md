# douyin-danmaku

纯 Rust 实现的抖音直播弹幕采集器，直连抖音 WebSocket（wss）接收弹幕流，不依赖系统代理、不需要开浏览器窗口。

## 能做什么

- 实时接收并打印直播间弹幕、礼物、进入、点赞、关注、粉丝团、热门弹幕等消息
- 支持**游客身份**（匿名，只带 `ttwid`）与**登录身份**（从本机 Edge 浏览器静默读取 Cookie）两种连接方式
- 签名在本地计算：内置抖音网页的 `sign.js`，由内嵌的 QuickJS 引擎执行，无需外部浏览器

## 运行

```powershell
# 直接运行（开发模式）
cargo run --package douyin-danmaku --bin douyin-danmaku

# 指定直播间：可以传短号，也可以直接粘直播间分享链接
cargo run --package douyin-danmaku --bin douyin-danmaku 123456789
cargo run --package douyin-danmaku --bin douyin-danmaku https://live.douyin.com/123456789

# 用本机 Edge 的登录 Cookie 连接（需已在 Edge 登录抖音）
cargo run --package douyin-danmaku --bin douyin-danmaku 123456789 --edge-cookie
```

不传房间号时，程序会提示你手动输入。构建产物在 `target/release/douyin-danmaku.exe`。

## 输出说明

程序启动后会先打印这样一段：

```
=== 纯 Rust 抖音弹幕采集器 ===
直播间: 123456789
身份: 游客

  ttwid: 1%7CFDGVBw_Er1kftiCA...
  room_id: 7706415355358497546
```

### 身份: 游客

指**未登录的匿名访客**身份。此时 WebSocket 连接只携带抖音下发的 `ttwid` Cookie，不带 `sessionid`、`sessionid_ss` 等登录态 Cookie。

- 能读到：公开弹幕、礼物、进入房间、点赞、关注、粉丝团、房间人数等公开信息
- 不能做：发送弹幕/评论、点赞、加入粉丝团等需要账号的互动操作（弹幕流里这些消息只会以他人的身份出现）
- 风控上更敏感：匿名连接更容易被限流或断开，长时间采集建议使用登录身份

加 `--edge-cookie` 参数即切换为**登录身份**：程序会复制一份本机 Edge 配置到临时目录，用无头 Edge 通过 CDP 读取**已解密**的抖音 Cookie（能兼容 Chrome 127+ 的 App-Bound Encryption），用完关闭 Edge 并删除临时目录，不影响你正在使用的浏览器。

### ttwid

抖音 Web 端下发的**匿名访客标识 Cookie**，形如 `1%7CFDGVBw_Er1kftiCA...`。

- `1%7C` 是 `1|` 的 URL 编码，后面跟一段服务端签名/加密串，整体作为 `Set-Cookie: ttwid=...` 下发
- 它相当于匿名会话的"设备身份"：WebSocket 连接必须带上它，否则服务端会拒绝连接或把连接降权
- 有效期由服务端控制（通常一天量级），过期后重新访问直播间页面即可拿到新的
- 程序里它来自访问直播间页面时响应头的 `Set-Cookie`，并在每次运行时重新获取

### room_id

直播间的**真实房间号**，一个 64 位大整数，例如 `7706415355358497546`。

它和你输入的**短号（web_rid）**不是一回事：

| 名称 | 例子 | 来源 |
| --- | --- | --- |
| `web_rid`（短号） | `123456789` | 直播间地址 `live.douyin.com/123456789` 里那个，是对外分享用的短号 |
| `room_id`（真实号） | `7706415355358497546` | 页面内部标识，WebSocket 订阅、`internal_ext` 等参数都要用它 |

程序流程是：用短号打开直播间页面 → 从 HTML 里提取 `roomId` → 用 `room_id` 拼接 WebSocket 地址并订阅该房间。弹幕消息里的 `wss_push_room_id`、`internal_ext` 也都是这个真实号。

## 参考项目

本项目的协议实现参考了开源项目 **[BarrageGrab](https://gitee.com/loveljsheng/BarrageGrab)**（C# / .NET 8 WPF，支持抖音、快手、视频号等多平台的 wss 弹幕直连）。它的抖音部分同样是自己解析 protobuf、自己拼 wss 地址、自己获取 `ttwid`，本项目的直连逻辑、`internal_ext` 拼装方式、`identity=audience` 等参数都与它一致。

两者主要差异：

| | BarrageGrab | douyin-danmaku |
| --- | --- | --- |
| 语言/形态 | C# / .NET 8 桌面 GUI（WPF） | Rust 命令行程序，单文件可执行 |
| 平台 | 抖音、快手、视频号、Tiktok、B 站等 | 仅抖音 |
| 签名 | `signature` 填占位值 `00000000` | 内嵌 `sign.js`，由 QuickJS 本地算出**真实签名** |
| 登录态 | 手工填入 Cookie | 从本机 Edge 静默读取已解密 Cookie |
| 输出 | 图形界面 + 本地 WebSocket 转发 | 终端直接打印 |

另外，签名参数的具体拼装顺序（13 个参数、顺序敏感）沿用自一个 Python 版本实现（`danmu_ws.py`），该 Python 脚本不在本仓库内，仅作为对照参考。

> 弹幕协议是抖音的私有协议，随时可能变。项目仅供学习研究使用，请遵守平台规则，不要用于商业或高频压测。

## 如何调试

- 跑测试（含签名冒烟测试与 protobuf 解析性能对比）：

  ```powershell
  cargo test --release --package douyin-danmaku --lib -- --nocapture
  ```

- 看更细的错误栈：

  ```powershell
  $env:RUST_BACKTRACE = "1"
  cargo run --package douyin-danmaku --bin douyin-danmaku 123456789
  ```

- 抓包对照：用浏览器开发者工具的 Network 面板过滤 `wss`，可以看到与本程序近似的 WebSocket 握手 URL 和参数。
