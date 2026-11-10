# 抖音直播间弹幕获取程序（Rust）实现计划

## 一、需求分析

用户希望使用 Rust 编写一个程序，能够获取指定抖音直播间的所有弹幕信息。

### 核心功能
- 根据直播间 URL 获取弹幕信息
- 实时接收并解析弹幕数据
- 支持弹幕输出和处理

## 二、技术调研结论

根据对抖音直播接口的调研，获取弹幕需要以下技术栈：

### 2.1 抖音直播弹幕接口分析

**接口类型**：WebSocket 长连接

**连接 URL**：
```
wss://webcast3-ws-web-hl.douyin.com/webcast/im/push/v2/
```

**核心参数**：
- `room_id`: 直播间 ID
- `signature`: 签名参数（动态生成）
- `cookie`: 包含 ttwid 等认证信息
- `app_name`: douyin_web
- `version_code`: 网页版版本号
- `webcast_sdk_version`: SDK 版本

**数据传输协议**：Protobuf（二进制格式）

### 2.2 参考项目

| 项目 | 语言 | 说明 | 本地副本 |
|------|------|------|------|
| DouyinLiveWebFetcher | Python | 网页版弹幕抓取，支持最新接口 | — |
| douyin-live-go | Golang | 高效直播数据采集工具 | — |
| biliup | Python | 支持抖音弹幕录制 | — |
| BarrageGrab | C#/.NET 8 | 抖音直播弹幕采集，协议定义最权威 | https://gitee.com/loveljsheng/BarrageGrab （GitHub 镜像 https://github.com/error1983/BarrageGrab ） |

### 2.3 技术难点

1. **签名生成**：`signature` 参数需要逆向抖音 JS 算法生成
2. **Cookie 获取**：需要用户提供有效的 Cookie（ttwid），可通过网页登录回调方式获取
3. **Protobuf 解析**：需要正确定义 protobuf 消息结构
4. **心跳保持**：WebSocket 连接需要定期发送心跳包

### 2.4 Cookie 获取方案

**方案：网页登录回调获取**

抖音网页版登录流程：
1. 用户打开浏览器访问抖音网页版登录页面
2. 通过扫码或手机号验证码登录
3. 登录成功后浏览器会设置 `ttwid` 等 Cookie
4. 程序通过本地 HTTP 服务器捕获回调，提取 Cookie

**实现方式**：
- 使用 `webbrowser` crate 打开默认浏览器访问抖音登录页面
- 使用本地 HTTP 服务器（如 `hyper`）监听回调地址
- 从回调请求中提取 Cookie（ttwid、sessionid 等）
- 将 Cookie 保存到配置文件供后续使用

**登录接口**：
- 抖音网页版登录入口：`https://www.douyin.com/`
- ttwid 获取接口：`https://ttwid.bytedance.com/ttwid/union/register/`（POST 请求，返回 ttwid）

## 三、架构设计

### 3.1 项目结构

> 本文档描述的是**立项时的设想结构**。实际落地后的目录如下（路径均以 `douyin-danmaku/` 为根目录）：

```
douyin-danmaku/
├── Cargo.toml / Cargo.lock      # Rust 项目配置与锁定依赖
├── README.md                    # 面向使用者的说明
├── AGENTS.md                    # 面向改代码者/AI 协作者的说明与实测协议结论
├── src/
│   ├── main.rs                  # 主入口：参数解析、重连与轮询主循环、缓冲输出
│   ├── lib.rs                   # 模块导出
│   ├── client.rs                # WebSocket 客户端（PushFrame/Response 解析、心跳、ack）
│   ├── sign.rs                  # 签名生成（signature）、ttwid/room_id 获取与缓存
│   ├── sign.js                  # 抖音网页签名脚本（由内嵌 QuickJS 执行）
│   ├── browser_cookie.rs        # 登录态 Cookie 获取（原本计划的 login.rs）
│   ├── models.rs                # 数据模型
│   └── config.rs                # 配置管理
├── tests/                       # 集成测试
├── examples/
├── docs/
│   └── douyin_danmaku_rust_plan.md   # 本文档
└── docs/                        # 说明文档（参考实现均为在线仓库，不入本仓库）
```

与原设想的差异：

| 原计划 | 实际落地 |
| --- | --- |
| `api.rs` 单独取 room_id | 并入 `sign.rs`（取 ttwid 与 roomId 必须在同一轮重试里完成，见下文对齐情况） |
| `login.rs` 用本地 HTTP 服务器捕获登录回调 | 改为 `browser_cookie.rs`：从本机 Edge（CDP）静默读取**已解密** Cookie；游客身份不需要登录 |
| `protobuf/webcast.proto` + prost | 改为手写零拷贝解析器（扁平字段表），不引入 prost，性能更好 |
| 依赖 `prost` / `prost-build` / `hyper` / `webbrowser` / `cookie` / `url` | 已移除，当前依赖更少、构建更快 |

### 3.2 核心模块职责

| 模块 | 职责 |
|------|------|
| `client.rs` | WebSocket 连接管理、心跳发送、PushFrame/Response 解析、ack 应答 |
| `sign.rs` | 生成 signature 参数；取 ttwid / roomId 并做本地缓存；页面请求头 |
| `browser_cookie.rs` | 从本机 Edge 静默读取登录 Cookie（原计划的 `login.rs`） |
| `models.rs` | 弹幕、礼物等数据结构定义与展示格式化 |
| `config.rs` | 连接配置（ws_url、ttwid、cookies 等） |
| `main.rs` | 参数解析、断线重连与未开播轮询主循环、缓冲输出 |

### 3.3 数据流

```
程序启动
       ↓
读取本地缓存的 ttwid / room_id（%LOCALAPPDATA%\douyin-danmaku\）
       ↓ (缺失)
sign.rs 请求直播间页面取 ttwid + roomId（带验证码风控重试）
       ↓ (--edge-cookie)
browser_cookie.rs 从本机 Edge 读取登录 Cookie
       ↓
用户输入直播间 URL / 短号
       ↓
sign.rs 生成 signature（QuickJS 执行 sign.js）
       ↓
client.rs 建立 WebSocket 连接（握手 Cookie 只带 ttwid）
       ↓
PushFrame / Response 解析（零拷贝）
       ↓
models.rs 结构化数据
       ↓
Out 缓冲输出弹幕信息
```

## 四、实现步骤

### 步骤 1：创建 Rust 项目

```bash
cargo new douyin-danmaku
cd douyin-danmaku
```

**依赖配置**（Cargo.toml）：
- `tokio`: 异步运行时
- `tokio-tungstenite`: WebSocket 客户端
- `reqwest`: HTTP 请求
- `prost`: Protobuf 解析
- `prost-build`: Protobuf 代码生成
- `serde` + `serde_json`: JSON 处理
- `url`: URL 解析
- `hyper`: 本地 HTTP 服务器（用于登录回调）
- `webbrowser`: 打开默认浏览器
- `cookie`: Cookie 解析

### 步骤 2：实现登录模块（获取 Cookie）

**功能**：
- 检测是否已有有效 Cookie
- 打开浏览器访问抖音登录页面
- 启动本地 HTTP 服务器监听登录回调
- 提取 ttwid 和 sessionid 等 Cookie
- 保存 Cookie 到配置文件

**实现细节**：
1. 调用 `https://ttwid.bytedance.com/ttwid/union/register/` 获取 ttwid（无需登录）
2. 打开浏览器访问抖音主页，用户扫码登录
3. 通过本地 HTTP 服务器捕获回调请求中的 Cookie

### 步骤 3：实现 API 模块（获取 room_id）

**功能**：
- 根据直播间 URL（如 `https://live.douyin.com/xxxx`）获取 room_id
- 通过抖音直播页面的 API 接口获取直播间信息

**关键接口**：
```
https://live.douyin.com/webcast/room/web/enter/
```

### 步骤 4：实现签名模块

**功能**：
- 逆向抖音网页版的 signature 生成算法
- 根据 URL 参数生成签名

**参考实现**：
参考 Python 项目中的签名算法，移植到 Rust

### 步骤 5：实现 WebSocket 客户端

**功能**：
- 建立 WebSocket 连接
- 发送初始化消息
- 定期发送心跳包
- 接收并处理服务器消息

**关键实现**：
- 使用 `tokio-tungstenite` 建立连接
- 处理二进制消息（Protobuf 格式）

### 步骤 6：实现 Protobuf 解析

**功能**：
- 定义抖音直播消息的 Protobuf 结构
- 解析接收到的二进制数据

**Protobuf 消息类型**：
- ChatMessage（弹幕消息）
- GiftMessage（礼物消息）
- LikeMessage（点赞消息）
- EnterMessage（进入消息）

### 步骤 7：实现主入口和配置

**功能**：
- 命令行参数解析
- 配置文件管理（Cookie）
- 弹幕输出

## 五、关键依赖

| 依赖 | 版本 | 用途 |
|------|------|------|
| tokio | 1.x | 异步运行时 |
| tokio-tungstenite | 0.21.x | WebSocket 客户端 |
| reqwest | 0.12.x | HTTP 请求 |
| prost | 0.12.x | Protobuf 解析 |
| prost-build | 0.12.x | Protobuf 代码生成 |
| serde | 1.x | 序列化/反序列化 |
| serde_json | 1.x | JSON 处理 |
| url | 2.x | URL 解析 |
| anyhow | 1.x | 错误处理 |
| hyper | 1.x | 本地 HTTP 服务器（登录回调） |
| webbrowser | 0.8.x | 打开默认浏览器 |
| cookie | 0.18.x | Cookie 解析和管理 |

## 六、风险与注意事项

### 6.1 接口变更风险

**问题**：抖音接口和签名算法可能随时变更

**应对措施**：
- 参考最新的开源项目获取接口信息
- 实现灵活的配置机制，便于更新参数
- 提供详细的错误日志，便于问题排查

### 6.2 Cookie 过期问题

**问题**：Cookie（ttwid）可能过期或被封禁

**应对措施**：
- 提示用户定期更新 Cookie
- 提供配置文件方式管理 Cookie

### 6.3 Protobuf 结构变更

**问题**：抖音可能修改 Protobuf 消息结构

**应对措施**：
- 保持 Protobuf 定义文件独立，便于更新
- 实现容错解析，忽略未知字段

## 七、测试计划

### 7.1 单元测试

- API 模块：测试 room_id 获取
- 签名模块：测试签名生成正确性
- Protobuf 模块：测试消息解析

### 7.2 集成测试

- 完整流程测试：连接直播间并接收弹幕
- 稳定性测试：长时间运行验证连接稳定性

## 八、交付物

1. **源代码**：完整的 Rust 项目
2. **配置文件**：示例配置和 Cookie 说明
3. **README**：使用说明和注意事项

## 九、预期完成时间

本计划仅包含技术方案和实现步骤，不包含具体时间估算。

### 与当前实现的对齐情况

本文档是**立项计划**，写于动手之前；项目随后按实测结论做了修正，现状以仓库根目录的 `README.md` 与 `AGENTS.md` 为准。

| 计划条目 | 当前状态 | 说明（均为实测） |
| --- | --- | --- |
| 步骤 1：创建 Rust 项目 | 已完成 | 依赖已全部升级到当前最新版（rquickjs 0.14 / tokio 1.x / reqwest 0.13 / rand 0.10 等） |
| 步骤 2：实现登录模块（获取 Cookie） | 换方案实现 | 计划中的「本地 HTTP 服务器捕获登录回调」未采用；改为 `browser_cookie.rs`（Edge CDP 静默读取），游客身份则完全不需要登录 |
| 步骤 3：实现 API 模块（获取 room_id） | 已完成，但换了接口 | 原计划的 `webcast/room/web/enter/` 需要签名参数，未使用；改为从直播间页面 HTML 提取 `roomId`，并与 ttwid 合并为同一重试循环 |
| 步骤 4：实现签名模块 | 已完成 | 用 QuickJS 执行 `src/sign.js`；签名参数 13 个、顺序敏感，取自参考项目 |
| 步骤 5：实现 WebSocket 客户端 | 已完成 | 额外做了断线重连（指数退避）、未开播轮询、心跳间隔跟随服务端 `heartbeatDuration`、`needAck` 应答（默认开启，不回应会被服务端 RST） |
| 步骤 6：实现 Protobuf 解析 | 已完成，换实现 | 未用 prost，改为零拷贝手写解析；帧结构按 `PushFrame`/`Response` 协议化解析 |
| 步骤 7：实现主入口和配置 | 已完成 | 另有 `--poll-secs` / `--max-retries` / `--verbose` / `--edge-cookie` 等开关，以及 ttwid/room_id 本地缓存 |
| 2.4 节的 ttwid 获取接口（`ttwid.bytedance.com` 注册接口） | **实测无效，未采用** | 该接口返回的是 bytedance/ixigua 域的 ttwid，带到 `live.douyin.com` 恒定拿到风控页；ttwid 只能来自直播间页面响应的 `Set-Cookie` |
| 2.4 节「网页登录回调」与「浏览器打开登录页」 | 未采用 | 改为从本机 Edge 直接读已解密 Cookie；游客身份只带 ttwid |

新增的、计划中没有的工程要点：

1. **WebSocket 握手 Cookie 只带 `ttwid`**：带 `msToken` / `__ac_nonce` 会被拒（`handshake-status: 417`、`not pass sign check`）。
2. **页面请求也只带 `ttwid`**：带 `msToken` 拿不到 `roomId`。
3. **直播间页面约三分之一概率返回 6KB 的「验证码中间页」**（抖音风控），必须识别并放慢重试。
4. **ttwid / room_id 本地缓存**（`%LOCALAPPDATA%\douyin-danmaku\`），让后续运行不再访问页面，从而绕开风控。
5. **`room_id` 是场次标识**，每次开播都会变，不能当直播间稳定唯一 ID。

更详细的现状说明见仓库根目录的 `README.md`（面向使用者）与 `AGENTS.md`（面向改代码者）。

---

**注意**：使用本程序需遵守抖音平台的使用条款和相关法律法规，建议仅用于个人学习和研究目的。