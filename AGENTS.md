# AGENTS.md

面向在本仓库内工作的 AI 代理与协作者的说明。**所有路径均以本仓库根目录（`douyin-danmaku/`）为基准**；这是单 crate 项目（包名与二进制同名 `douyin-danmaku`），没有 workspace，直接在根目录跑 cargo 即可。

## 项目是什么

纯 Rust 实现的抖音直播弹幕采集器：直连抖音 WebSocket（wss）接收弹幕流，签名在本地用内嵌 QuickJS 执行抖音网页的 `sign.js` 计算，不需要开浏览器窗口、不依赖系统代理。

面向使用者看的是 `README.md`（功能、运行方式、参数、参考项目对照）；本文件面向**改代码的人**。

## 目录结构

```
douyin-danmaku/
├── AGENTS.md                    # 本文件
├── README.md                    # 面向使用者的说明
├── Cargo.toml / Cargo.lock
├── src/
│   ├── main.rs                  # 入口：参数解析、重连/轮询主循环、缓冲输出
│   ├── lib.rs                   # 模块导出
│   ├── client.rs                # WebSocket 连接、PushFrame/Response 解析、心跳、ack
│   ├── sign.rs                  # 签名、ttwid/room_id 获取与缓存、页面请求头
│   ├── browser_cookie.rs        # 从本机 Edge（CDP）静默读取登录 Cookie
│   ├── models.rs                # 消息类型与展示格式化
│   ├── config.rs                # 连接配置结构
│   └── sign.js                  # 抖音网页签名脚本（由 QuickJS 执行）
├── tests/                       # 集成测试（当前为空，预留）
├── examples/                    # 示例程序（当前为空，预留）
└── docs/
    └── douyin_danmaku_rust_plan.md   # 最初的实现计划（历史文档）
```

- 本仓库**不包含任何第三方代码副本**。参考实现一律用在线仓库地址查阅：
  - [BarrageGrab](https://gitee.com/loveljsheng/BarrageGrab)（C#/.NET 8，MIT，gitee 主仓库）｜[error1983/BarrageGrab](https://github.com/error1983/BarrageGrab)（GitHub 镜像）
  - 权威 proto：[BarrageGrab.Entity/Protobuf/Douyin/Douyin.proto](https://github.com/error1983/BarrageGrab/blob/master/BarrageGrab.Entity/Protobuf/Douyin/Douyin.proto)
  - Cookie/ttwid 相关：[GrabServices/DouyinBarrageGrabService.cs](https://github.com/error1983/BarrageGrab/blob/master/BarrageGrab/GrabServices/DouyinBarrageGrabService.cs)
- 需要离线查阅时，可自行 clone 到工作区外的任意目录；**不要**把它放进本仓库的构建或版本管理。
- `docs/douyin_danmaku_rust_plan.md` 是立项时的计划文档，与当前实现已有出入（见该文档末尾的「与当前实现的对齐情况」）。

## 常用命令

```powershell
# 构建（跑 exe 前先停掉旧进程，否则 target\release\douyin-danmaku.exe 会被占用）
cargo build --release

# 测试（lib + bin 单测）
cargo test --release -- --nocapture

# 运行：`--` 之后原样传给程序；也可直接跑已构建的 exe
cargo run --release -- 587795807816 --poll-secs 5
.\target\release\douyin-danmaku.exe 587795807816 --poll-secs 5
```

## 必须知道的协议结论（均为实测，勿凭记忆推翻）

1. **WebSocket 握手 Cookie 只带 `ttwid`**。带上 `msToken` / `__ac_nonce` 会被服务端拒绝：`handshake-status: 417`、`handshake-msg: "not pass sign check"`，表现为无限重连刷屏。
2. **页面请求也只带 `ttwid`**（还没拿到 ttwid 时不带任何 Cookie）。带 `msToken` 拿不到 `roomId`。
3. **`ttwid` 只能来自 `live.douyin.com` 页面响应的 `Set-Cookie`**。`ttwid.bytedance.com/ttwid/union/register/` 虽然稳定返回，但那是 bytedance/ixigua 域的值，对 live.douyin.com 无效。
4. **直播间页面约三分之一概率返回约 6KB 的「验证码中间页」**（抖音风控，标题含「验证码中间页」），里面既没有 `roomId` 也没有 `ttwid` 头。程序识别该页并放慢重试（间隔 5 秒）。
5. `ttwid` 与 `room_id` 缓存在 `%LOCALAPPDATA%\douyin-danmaku\`，跨次运行复用，避免再触页面；握手被判失效时自动清除缓存重取。
6. **帧结构**：`PushFrame{seqId=1, logId=2, payloadType=7, payload=8}` → gzip → `Response{messagesList=1, cursor=2, internalExt=5, heartbeatDuration=8, needAck=9, pushServer=10}`；`Message{method=1, payload=2}`。心跳字节 `[0x3a, 0x02, 0x68, 0x62]`。
7. **`room_id` 是场次标识**，每次开播都会变，不能当直播间稳定唯一 ID（短号才是）。
8. 心跳间隔跟随服务端 `heartbeatDuration`（钳制 1~120 秒，>120 视为毫秒）。
9. **`needAck` 必须回应，否则服务端直接 RST 连接**（不发送 WebSocket close 帧，tungstenite 报 `Connection reset without closing handshake`）。实测：不回应时 200 秒内被断 8 次，回应后 200 秒内 0 次断连。因此应答**默认开启**，用 `--no-ack` 关闭（`--ack` 保留为兼容写法，等价于默认行为）。

## 代码约定

- 帧解析走**协议化**结构（PushFrame/Response），不要回退到「扫描 gzip magic 猜帧结构」。
- protobuf 解析走**零拷贝**扁平字段表，别改回 `HashMap<u32, Vec<...>>`（回归测试 `protobuf_zero_copy_matches_legacy_and_is_faster` 会对比旧实现并校验速度）。
- 输出统一经 `Out`（64KB `BufWriter`，攒够 32 行或距上次刷新 ≥300ms 即落盘），所有事件经同一回调以保证顺序。
- 签名日志只在 `--verbose` 时打印，否则重连会刷屏。
- 新增协议字段前先查上游 [Douyin.proto](https://github.com/error1983/BarrageGrab/blob/master/BarrageGrab.Entity/Protobuf/Douyin/Douyin.proto)（或 gitee 镜像的同名文件），不要凭记忆写字段号。

## 提交约定

- 版本管理用本仓库内的 git（分支 `main`），每项优化独立提交，提交信息写清「做了什么 + 借鉴了哪个参考项目」。
- 交付时给出**可执行文件路径**（`target/release/douyin-danmaku.exe`）与本次改动对应的 commit。
- README 的「参考项目与实现对照」需同步更新：新增能力写进「已参考并落地的部分」，只调研没实现的写进「已调研、暂未实现」。
- 删除文件用 `C:\Users\Admin\.minimax\bin\mavis-trash.cmd`（`Remove-Item` 会被安全策略拦截）。
- `.gitignore` 改动请用 shell 写入（对 `.gitignore` 用 write/edit 会 EPERM）。
- 本机可无头运行的 exe 只有本项目；跑测试不必先停进程，只有 `cargo build` 覆盖 exe 时才需要。

## 环境注意

- Windows + PowerShell。命令行里不要出现会破坏 JSON 传参的转义写法。
- `cargo build` 前先 `Get-Process douyin-danmaku -ErrorAction SilentlyContinue | Stop-Process -Force`，否则报「拒绝访问 target\release\douyin-danmaku.exe」。
- bash 超时不会自动杀掉 exe 进程，需手动 `Stop-Process`。
