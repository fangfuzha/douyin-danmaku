# douyin-danmaku

纯 Rust 实现的抖音直播弹幕采集器，直连抖音 WebSocket（wss）接收弹幕流，不依赖系统代理、不需要开浏览器窗口。

## 能做什么

- 实时接收并打印直播间弹幕、礼物、进入、点赞、关注、粉丝团、热门弹幕等消息
- 支持**游客身份**（匿名，只带 `ttwid`）与**登录身份**（从本机 Edge 浏览器静默读取 Cookie）两种连接方式
- 签名在本地计算：内置抖音网页的 `sign.js`，由内嵌的 QuickJS 引擎执行，无需外部浏览器
- **断线自动重连**（指数退避），**未开播时自动轮询等待开播**
- 心跳间隔自适应服务端下发的 `heartbeatDuration`
- 自动回应服务端 `needAck`（不回应会被服务端直接断开连接）

## 项目结构

所有路径以本仓库根目录 `douyin-danmaku/` 为基准：

```
douyin-danmaku/
├── README.md          # 本文件：功能、运行方式、参考项目对照
├── AGENTS.md          # 给改代码的人/AI 协作者看的说明与协议结论
├── src/               # Rust 源码
├── tests/             # 集成测试（当前为空，预留）
├── examples/          # 示例程序（当前为空，预留）
├── docs/
│   └── douyin_danmaku_rust_plan.md   # 立项时的实现计划（历史文档）
└── Cargo.toml
```

本项目是**单 crate**（包名 `douyin-danmaku`，二进制同名），没有 workspace，直接在本目录用 cargo 命令即可。

参考实现全部为**在线开源仓库**，本仓库不包含任何第三方代码副本，核对协议字段时直接看在线源码：

- [BarrageGrab](https://gitee.com/loveljsheng/BarrageGrab)（C#/.NET 8，MIT，gitee 主仓库）｜[error1983/BarrageGrab](https://github.com/error1983/BarrageGrab)（GitHub 镜像）
- 权威 proto：[BarrageGrab.Entity/Protobuf/Douyin/Douyin.proto](https://github.com/error1983/BarrageGrab/blob/master/BarrageGrab.Entity/Protobuf/Douyin/Douyin.proto)

## 运行

```powershell
# 直接运行（开发模式）；`--` 之后的内容原样传给程序
cargo run --release -- 123456789

# 也可以传直播间分享链接
cargo run --release -- https://live.douyin.com/123456789

# 用本机 Edge 的登录 Cookie 连接（需已在 Edge 登录抖音）
cargo run --release -- 123456789 --edge-cookie
```

不传房间号时，程序会提示你手动输入。构建产物在 `target/release/douyin-danmaku.exe`，也可以先 `cargo build --release` 再直接运行该 exe。

### 命令行参数

| 参数 | 默认值 | 说明 |
| --- | --- | --- |
| `<房间号或链接>` | 交互式询问 | 直播间短号（web_rid）或分享链接 |
| `--edge-cookie` / `--edge` | 关闭 | 从本机 Edge 静默读取登录 Cookie |
| `--poll-secs <秒>` | `10` | 未开播时的轮询间隔 |
| `--max-retries <次数>` | `0`（无限） | 断线重连的最大次数，0 表示一直重连 |
| `--verbose` / `-v` | 关闭 | 打印签名中间值（MD5、signature）等调试信息 |
| `--no-ack` | 关闭 | **不要**回应服务端 `needAck`（仅用于复现「不应答」行为，默认应答） |

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
- 程序里它来自访问直播间页面时响应头的 `Set-Cookie`，并在每次运行时重新获取；断线重连多次后也会自动刷新
- 服务端并非每次响应都下发 `ttwid`（实测约一半概率有），且直播间页面本身约三分之一概率只返回约 6KB 的**验证码中间页**（抖音风控，里面没有 `roomId`）。程序因此把"取 ttwid + 取 roomId"放在同一个重试循环里，最多重试 8 次（遇到验证码页时把间隔拉长到 5 秒，避免越试越被拦）
- `ttwid` 与 `room_id` 会缓存在本地（`%LOCALAPPDATA%\douyin-danmaku\`），下一次运行直接复用，不再访问直播间页面，也就绕开了验证码风控；缓存的 `ttwid` 若被服务端判为失效（握手返回 `not pass sign check`），程序会自动清除并重新获取
- 请求直播间页面时**只带 `ttwid`**（还没有 ttwid 时不带任何 Cookie）。实测：带上 `msToken`（BarrageGrab 用的 107 位随机串）或 `__ac_nonce` 时拿不到 `roomId`；带任意 Cookie 时服务端也不再下发 `ttwid`

### room_id

直播间的**真实房间号**，一个 64 位大整数，例如 `7706415355358497546`。

它和你输入的**短号（web_rid）**不是一回事：

| 名称 | 例子 | 来源 |
| --- | --- | --- |
| `web_rid`（短号） | `123456789` | 直播间地址 `live.douyin.com/123456789` 里那个，是对外分享用的短号 |
| `room_id`（真实号） | `7706415355358497546` | 页面内部标识，WebSocket 订阅、`internal_ext` 等参数都要用它 |

程序流程是：用短号打开直播间页面 → 从 HTML 里提取 `roomId` → 用 `room_id` 拼接 WebSocket 地址并订阅该房间。弹幕消息里的 `wss_push_room_id`、`internal_ext` 也都是这个真实号。

> ⚠️ `room_id` 是**场次标识**，主播每次开播都会变，不能当作直播间的稳定唯一标识。要长期跟踪同一个主播，请记录它的 `sec_uid` 或主页地址。（这一点来自 DouyinBarrageGrab 的作者说明。）

## 参考项目与实现对照

本项目的协议与工程实现参考了下面这些开源项目。下表列出**每一项优化分别借鉴了谁**，方便日后本项目失效时按图索骥地修复。

### 已参考并落地的部分

| 本项目的能力 | 参考项目 | 具体借鉴内容 |
| --- | --- | --- |
| wss 直连、`internal_ext` 拼装、`identity=audience`、自取 `ttwid` | [BarrageGrab](https://gitee.com/loveljsheng/BarrageGrab)（C#/.NET 8）、[error1983/BarrageGrab](https://github.com/error1983/BarrageGrab)（GitHub 镜像） | 抖音直播的连接参数拼装方式 |
| 帧结构（PushFrame / Response 字段号） | BarrageGrab 的 [`BarrageGrab.Entity/Protobuf/Douyin/Douyin.proto`](https://github.com/error1983/BarrageGrab/blob/master/BarrageGrab.Entity/Protobuf/Douyin/Douyin.proto)（在线） | `seqId=1`、`logId=2`、`payloadType=7`、`payload=8`；`messagesList=1`、`heartbeatDuration=8`、`needAck=9` |
| 抗风控：识别验证码中间页、放慢重试、缓存 `ttwid` / `room_id` | [lihuanpeng/DouyinBarrageGrab](https://github.com/lihuanpeng/DouyinBarrageGrab)（C#，MIT） | 实测抖音会对高频访问直播间页面的来源返回"验证码中间页"（约 6KB），本项目据此放慢重试并把 `ttwid`/`room_id` 缓存到本地，后续运行不再触碰页面 |
| Cookie 只带 `ttwid`（页面请求与 WebSocket 握手都是） | BarrageGrab（[`GrabServices/DouyinBarrageGrabService.cs`](https://github.com/error1983/BarrageGrab/blob/master/BarrageGrab/GrabServices/DouyinBarrageGrabService.cs) 的 `GetTtwid` / `GetRoomId`，握手头 `cookie: ttwid={Ttwid}`）、[saermart/DouyinLiveWebFetcher](https://github.com/saermart/DouyinLiveWebFetcher)（`liveMan.py` 握手 cookie 同样是 `ttwid={self.ttwid}`） | **实测**：① 握手若带上 msToken/`__ac_nonce` 会被服务端拒绝（`handshake-status: 417`、`handshake-msg: "not pass sign check"`）并陷入重连循环，只带 ttwid 才能握手成功（状态 101）；② 页面请求带上 msToken 会让服务端几乎恒定返回空壳页（拿不到 `roomId`），因此本项目**未采用** BarrageGrab 的 msToken/`__ac_nonce` 组合（详见「已调研、暂未实现」） |
| 签名参数（13 个、顺序敏感） | [saermart/DouyinLiveWebFetcher](https://github.com/saermart/DouyinLiveWebFetcher)（Python）、[littlezo/dy-danmu](https://github.com/littlezo/dy-danmu)（Go） | `frontierSign` / `sign.js` 的签名思路；dy-danmu 用 Go 的 JS 解释器算签名，与本项目用 QuickJS 跑 `sign.js` 是同一路数 |
| protobuf 解析 | DouyinLiveWebFetcher 的 `douyin.proto`、dyhub 的消息类型枚举 | 消息体字段定义 |
| 心跳间隔跟随服务端 | [Simple Live 的协议拆解](https://blog.gitcode.com/0268abd9f86d85f8609446d3d803368e.html) | 服务端 `heartbeatDuration` 下发后按其心跳，客户端把间隔限制在 1~120 秒 |
| `needAck` 应答 | 同上（Simple Live 在 `needAck` 为真时回 `payloadType='ack'` 帧） | 本项目**默认开启**应答（`--no-ack` 可关闭）。**实测**：不回应时服务端会直接 RST 掉连接（无 close 帧，报 `Connection reset without closing handshake`），200 秒内被断 8 次；回应后 200 秒内 0 次断连 |
| 断线重连 | [jwwsjlm/douyinLive](https://github.com/jwwsjlm/douyinLive)（Go，LiveTap 的采集内核）、[tiangalon/dy_danmaku_java](https://github.com/tiangalon/dy_danmaku_java)（Java） | 断线自动重连 + 退避间隔 + 未开播轮询 + 保活 |
| 抗风控/稳定性思路 | [lihuanpeng/DouyinBarrageGrab](https://github.com/lihuanpeng/DouyinBarrageGrab)（C#，MIT） | 轮询兜底模式、`room_id` 是场次标识的提醒 |
| 事件类型枚举（表情弹幕、富文本等） | [haohanliubu/dydy](https://github.com/haohanliubu/dydy)（TypeScript/Vue） | `WebcastEmojiChatMessage`、`WebcastRoomDataSyncMessage`、`CastRtfContent` 等类型定义 |

### 已调研、暂未实现（留作后续修复时的备选）

如果本项目某天失效（抖音改了协议），下面这些是值得直接借鉴的现成实现，按"改动量从小到大"排列：

| 能力 | 可借鉴的项目 | 说明 |
| --- | --- | --- |
| msToken / `__ac_nonce` Cookie 组合 | BarrageGrab（[`GrabServices/DouyinBarrageGrabService.cs`](https://github.com/error1983/BarrageGrab/blob/master/BarrageGrab/GrabServices/DouyinBarrageGrabService.cs) 的 `GenerateMsToken`：107 位随机串；`__ac_nonce`：`0` + 20 位随机串） | **实测未采用**：页面请求带上 msToken 拿不到 `roomId`，握手带上 msToken/`__ac_nonce` 则被拒（417 not pass sign check）。若将来抖音放开这两项，可直接照 BarrageGrab 补上 |
| IM 预取接口 | Simple Live 的协议拆解、jwwsjlm/douyinLive | 连接前先请求 `https://live.douyin.com/webcast/im/fetch/`，从响应里取 `pushServer` / `cursor` / `internalExt` / `heartbeatDuration`，比本项目硬编码 `webcast100-ws-web-lq` 域名与手工拼 `internal_ext` 更稳 |
| 备用推送域名 | Simple Live、dyhub | 预生成 `webcast3/webcast5` 多组域名，连接失败自动切换并刷新上下文 |
| 多房间并发 | jwwsjlm/douyinLive、dyhub | 每个房间独立任务与状态机，节点可横向扩容 |
| 统一事件协议 + 分发 | [ymstar/dyhub](https://github.com/ymstar/dyhub)（TypeScript，2026-11 仍在更新） | 抖音私有 protobuf → 标准化 `DanmakuEvent`，再经 WS / SSE / Webhook 三通道分发；"上游改协议不碰消费端"的解耦思路很值得抄 |
| 服务化与可观测 | jwwsjlm/douyinLive、LiveTap | 只读 HTTP API、批量状态查询、URL 解析、Prometheus 指标 |
| 落库与后台 | littlezo/dy-danmu、[cyber-xiaoya/cyber-xiaoya-danmaku](https://github.com/cyber-xiaoya/cyber-xiaoya-danmaku) | 弹幕/礼物入库、Web 后台、数字人桥接 |
| 轮询模式兜底 | lihuanpeng/DouyinBarrageGrab 的 `forcePolling` | ws 断连时改用 HTTP 轮询，慢但绝对稳定 |
| 多语言 SDK | [lulajax/DouyinLiveJava](https://github.com/lulajax/DouyinLiveJava)、[Vz7797/LiveTap](https://github.com/Vz7797/LiveTap) | 事件分发模型与 SDK 形态参考 |
| 签名简化 | littlezo/dy-danmu | 用 JS 解释器直接调 `frontierSign()`，可省掉整个 `sign.js` 与 QuickJS 依赖 |

> 弹幕协议是抖音的私有协议，随时可能变。项目仅供学习研究使用，请遵守平台规则，不要用于商业或高频压测。

## 如何调试

- 遇到「触发了抖音的验证码风控」：说明短时间内访问直播间页面太频繁。程序已经放慢重试并把 `ttwid`/`room_id` 缓存到本地，等几分钟再跑即可；想立刻重来可以删掉 `%LOCALAPPDATA%\douyin-danmaku\` 下的缓存
- 遇到「`not pass sign check`」：说明服务端判定当前 `ttwid` 无效，程序会自动清掉缓存重新获取，无需手动处理

- 跑测试（含签名冒烟、帧协议解析、protobuf 解析性能对比）：

  ```powershell
  cargo test --release -- --nocapture
  ```

- 看更细的错误栈：

  ```powershell
  $env:RUST_BACKTRACE = "1"
  cargo run --release -- 123456789
  ```

- 抓包对照：用浏览器开发者工具的 Network 面板过滤 `wss`，可以看到与本程序近似的 WebSocket 握手 URL 和参数。
