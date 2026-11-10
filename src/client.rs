use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use tokio::time::{self, Duration};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message as WsMessage};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use flate2::read::GzDecoder;
use std::io::Read;

use crate::config::WsSignConfig;
use crate::models::MessageType;

// ---------------------------------------------------------------------------
// protobuf 读取（零拷贝：bytes 字段直接借用原始缓冲区）
// ---------------------------------------------------------------------------

fn read_varint(data: &[u8], pos: usize) -> Option<(u64, usize)> {
    let mut result: u64 = 0;
    let mut shift = 0;
    let mut p = pos;
    while p < data.len() {
        let b = data[p];
        result |= ((b & 0x7f) as u64) << shift;
        p += 1;
        if (b & 0x80) == 0 {
            return Some((result, p));
        }
        shift += 7;
        if shift >= 64 {
            return None;
        }
    }
    None
}

/// protobuf 字段值。Bytes 直接借用原始缓冲区，避免每条消息复制一遍负载。
#[derive(Debug, Clone, Copy)]
enum PbValue<'a> {
    Varint(u64),
    Bytes(&'a [u8]),
    Fixed64,
    Fixed32,
}

/// 解析后的 protobuf 字段集合：扁平存放 (字段号, wire type, 值)。
/// 字段数量很少，线性查找比 HashMap 更快，且省掉哈希表与多层 Vec 的分配。
struct Fields<'a> {
    entries: Vec<(u32, u32, PbValue<'a>)>,
}

impl<'a> Fields<'a> {
    #[inline]
    fn bytes(&self, field: u32) -> Option<&'a [u8]> {
        self.entries.iter().find_map(|&(f, wt, v)| {
            if f == field && wt == 2 {
                match v {
                    PbValue::Bytes(b) => Some(b),
                    _ => None,
                }
            } else {
                None
            }
        })
    }

    #[inline]
    fn varint(&self, field: u32) -> Option<u64> {
        self.entries.iter().find_map(|&(f, wt, v)| {
            if f == field && wt == 0 {
                match v {
                    PbValue::Varint(n) => Some(n),
                    _ => None,
                }
            } else {
                None
            }
        })
    }

    /// 按字段号取出全部 bytes 字段（repeated）
    #[inline]
    fn bytes_all(&self, field: u32) -> impl Iterator<Item = &'a [u8]> + '_ {
        self.entries.iter().filter_map(move |&(f, wt, v)| {
            if f == field && wt == 2 {
                match v {
                    PbValue::Bytes(b) => Some(b),
                    _ => None,
                }
            } else {
                None
            }
        })
    }
}

fn parse_protobuf<'a>(data: &'a [u8]) -> Fields<'a> {
    let mut entries = Vec::with_capacity(8);
    let mut pos = 0usize;
    while pos < data.len() {
        let Some((tag, new_pos)) = read_varint(data, pos) else {
            break;
        };
        pos = new_pos;
        let field_number = (tag >> 3) as u32;
        let wire_type = (tag & 7) as u32;

        let value = match wire_type {
            0 => {
                let Some((v, np)) = read_varint(data, pos) else {
                    break;
                };
                pos = np;
                PbValue::Varint(v)
            }
            1 => {
                if pos + 8 > data.len() {
                    break;
                }
                pos += 8;
                PbValue::Fixed64
            }
            2 => {
                let Some((length, np)) = read_varint(data, pos) else {
                    break;
                };
                pos = np;
                let len = length as usize;
                if pos + len > data.len() {
                    break;
                }
                let val = &data[pos..pos + len];
                pos += len;
                PbValue::Bytes(val)
            }
            5 => {
                if pos + 4 > data.len() {
                    break;
                }
                pos += 4;
                PbValue::Fixed32
            }
            _ => break,
        };

        entries.push((field_number, wire_type, value));
    }
    Fields { entries }
}

// ---------------------------------------------------------------------------
// protobuf 写入（仅用于回发 ack 帧）
// ---------------------------------------------------------------------------

fn write_varint(out: &mut Vec<u8>, mut v: u64) {
    loop {
        let b = (v & 0x7f) as u8;
        v >>= 7;
        if v == 0 {
            out.push(b);
            break;
        }
        out.push(b | 0x80);
    }
}

fn write_varint_field(out: &mut Vec<u8>, field: u32, v: u64) {
    write_varint(out, ((field as u64) << 3) | 0);
    write_varint(out, v);
}

fn write_bytes_field(out: &mut Vec<u8>, field: u32, data: &[u8]) {
    write_varint(out, ((field as u64) << 3) | 2);
    write_varint(out, data.len() as u64);
    out.extend_from_slice(data);
}

// ---------------------------------------------------------------------------
// 帧结构
//
// 服务端下发的每一帧是一个 PushFrame（参考 BarrageGrab 的 Douyin.proto）：
//   1 seqId / 2 logId / 7 payloadType / 8 payload
// payload 通常是 gzip 压缩后的 Response：
//   1 messagesList / 2 cursor / 5 internalExt / 8 heartbeatDuration / 9 needAck
// ---------------------------------------------------------------------------

const PUSHFRAME_SEQ_ID: u32 = 1;
const PUSHFRAME_LOG_ID: u32 = 2;
const PUSHFRAME_PAYLOAD_TYPE: u32 = 7;
const PUSHFRAME_PAYLOAD: u32 = 8;

const RESPONSE_MESSAGES: u32 = 1;
const RESPONSE_HEARTBEAT_DURATION: u32 = 8;
const RESPONSE_NEED_ACK: u32 = 9;

const GZIP_MAGIC: [u8; 2] = [0x1f, 0x8b];

/// 一帧解析出来的会话状态
struct FrameState {
    /// 服务端指定的心跳间隔（秒），None 表示服务端没给
    heartbeat_secs: Option<u64>,
    /// 服务端要求回 ack
    need_ack: bool,
    /// 当前帧的 seqId / logId，用于构造 ack 帧
    seq_id: u64,
    log_id: u64,
}

/// 解码为 UTF-8 字符串（借用），仅接受可见字符。
fn decode_string(data: &[u8]) -> Option<&str> {
    let s = std::str::from_utf8(data).ok()?;
    if s.chars().all(|c| c >= ' ' || c == '\n' || c == '\r' || c == '\t') {
        Some(s)
    } else {
        None
    }
}

/// 构造 ack 确认帧：PushFrame { seqId, logId, payloadType = "ack" }
fn build_ack_frame(seq_id: u64, log_id: u64) -> Vec<u8> {
    let mut out = Vec::with_capacity(32);
    write_varint_field(&mut out, PUSHFRAME_SEQ_ID, seq_id);
    write_varint_field(&mut out, PUSHFRAME_LOG_ID, log_id);
    write_bytes_field(&mut out, PUSHFRAME_PAYLOAD_TYPE, b"ack");
    out
}

/// 把服务端给的心跳间隔换算成秒，并限制在合理范围内。
/// 参考 Simple Live 的处理：服务端值可能被夹在 1~120 秒之间。
fn normalize_heartbeat(raw: u64) -> u64 {
    let secs = if raw > 120 { raw / 1000 } else { raw };
    secs.clamp(1, 120)
}

fn extract_user(user_bytes: &[u8]) -> (String, String) {
    let fields = parse_protobuf(user_bytes);
    let user_id = fields.varint(1).map(|v| v.to_string()).unwrap_or_default();
    let nick_name = fields
        .bytes(3)
        .and_then(decode_string)
        .unwrap_or_default()
        .to_owned();
    (nick_name, user_id)
}

fn extract_chat_message(payload: &[u8]) -> (String, String) {
    let fields = parse_protobuf(payload);
    let content = fields
        .bytes(3)
        .and_then(decode_string)
        .unwrap_or_default()
        .to_owned();

    let (nick_name, user_id) = match fields.bytes(2) {
        Some(user_bytes) => extract_user(user_bytes),
        None => (String::new(), String::new()),
    };

    let user = if !nick_name.is_empty() {
        nick_name
    } else if !user_id.is_empty() {
        user_id
    } else {
        "未知".to_string()
    };
    (user, content)
}

fn extract_member_message(payload: &[u8]) -> (String, u64) {
    let fields = parse_protobuf(payload);
    let member_count = fields.varint(3).unwrap_or(0);
    let (nick_name, _) = match fields.bytes(2) {
        Some(user_bytes) => extract_user(user_bytes),
        None => (String::new(), String::new()),
    };
    (nick_name, member_count)
}

fn extract_gift_message(payload: &[u8]) -> (String, String, u64) {
    let fields = parse_protobuf(payload);
    let (nick_name, _) = match fields.bytes(2) {
        Some(user_bytes) => extract_user(user_bytes),
        None => (String::new(), String::new()),
    };
    let gift_name = fields
        .bytes(3)
        .and_then(decode_string)
        .unwrap_or_default()
        .to_owned();
    let gift_count = fields.varint(4).unwrap_or(1);
    (nick_name, gift_name, gift_count)
}

fn extract_like_message(payload: &[u8]) -> (String, u64) {
    let fields = parse_protobuf(payload);
    let (nick_name, _) = match fields.bytes(2) {
        Some(user_bytes) => extract_user(user_bytes),
        None => (String::new(), String::new()),
    };
    let like_count = fields.varint(3).unwrap_or(1);
    (nick_name, like_count)
}

fn extract_lottery_message(payload: &[u8]) -> Option<String> {
    let fields = parse_protobuf(payload);
    let common_bytes = fields.bytes(1)?;
    let common_fields = parse_protobuf(common_bytes);
    if common_fields.varint(14) != Some(2) {
        return None;
    }
    fields
        .bytes(3)
        .and_then(decode_string)
        .filter(|s| !s.is_empty())
        .map(|s| s.to_owned())
}

/// 解析社交消息（关注、分享等）
fn extract_social_message(payload: &[u8]) -> (String, String) {
    let fields = parse_protobuf(payload);
    let (nick_name, _) = match fields.bytes(2) {
        Some(user_bytes) => extract_user(user_bytes),
        None => (String::new(), String::new()),
    };
    // 字段 1 是 CommonTextMessage，其中字段 2 是内容
    let action = fields
        .bytes(1)
        .map(|common_bytes| {
            let common_fields = parse_protobuf(common_bytes);
            common_fields
                .bytes(2)
                .and_then(decode_string)
                .unwrap_or_default()
                .to_owned()
        })
        .unwrap_or_default();
    let action = if action.is_empty() {
        "分享了直播间".to_string()
    } else {
        action
    };
    (nick_name, action)
}

/// 解析粉丝团消息
fn extract_fansclub_message(payload: &[u8]) -> (String, String, i32, String) {
    let fields = parse_protobuf(payload);
    let (nick_name, _) = match fields.bytes(2) {
        Some(user_bytes) => extract_user(user_bytes),
        None => (String::new(), String::new()),
    };
    // 粉丝团名称和等级位于同一个嵌套消息里，只解析一次
    let (club_name, level) = match fields.bytes(1) {
        Some(club_bytes) => {
            let club_fields = parse_protobuf(club_bytes);
            let name = club_fields
                .bytes(1)
                .and_then(decode_string)
                .unwrap_or_default()
                .to_owned();
            let level = club_fields.varint(2).unwrap_or(0) as i32;
            (name, level)
        }
        None => (String::new(), 0),
    };
    let action = match fields.varint(3) {
        Some(1) => "加入了粉丝团".to_string(),
        Some(2) => "升级了粉丝团".to_string(),
        _ => "粉丝团变动".to_string(),
    };
    (nick_name, club_name, level, action)
}

/// 解析热门弹幕消息
fn extract_hot_chat_message(payload: &[u8]) -> String {
    let fields = parse_protobuf(payload);
    // 尝试从嵌套结构中提取内容
    for field_num in [1, 2, 3] {
        if let Some(b) = fields.bytes(field_num) {
            if let Some(s) = decode_string(b) {
                if !s.is_empty() {
                    return s.to_owned();
                }
            }
            // 尝试嵌套解析
            let sub_fields = parse_protobuf(b);
            for sub_field in [1, 2, 3] {
                if let Some(sb) = sub_fields.bytes(sub_field) {
                    if let Some(s) = decode_string(sb) {
                        if !s.is_empty() {
                            return s.to_owned();
                        }
                    }
                }
            }
        }
    }
    String::new()
}

fn extract_room_user_seq(payload: &[u8]) -> (String, u64) {
    let fields = parse_protobuf(payload);
    let online = fields
        .bytes(10)
        .and_then(decode_string)
        .unwrap_or("0")
        .to_owned();
    let total = fields.varint(3).unwrap_or(0);
    (online, total)
}

pub struct DanmakuClient {
    /// 是否回发 needAck 确认帧（协议实验特性，默认关闭）
    send_ack: bool,
}

impl DanmakuClient {
    pub fn new() -> Self {
        DanmakuClient { send_ack: false }
    }

    /// 开启 needAck 应答。
    ///
    /// 服务端在 Response.needAck 为 true 时会要求客户端回一个 payloadType="ack" 的帧。
    /// 参考 Simple Live 的实现；BarrageGrab / DouyinLiveWebFetcher 不回 ack 也能正常工作，
    /// 因此默认关闭，出现异常时可随时关掉。
    pub fn with_ack(mut self, enable: bool) -> Self {
        self.send_ack = enable;
        self
    }

    /// 建立一次连接并持续接收消息，直到连接关闭或出错。
    ///
    /// 断线重连与未开播轮询由调用方（main.rs）根据返回的错误决定。
    pub async fn connect_once<F>(&self, sign_config: &WsSignConfig, mut callback: F) -> Result<u64>
    where
        F: FnMut(MessageType) + Send,
    {
        callback(MessageType::System(format!(
            "使用真实签名直连 WebSocket (room_id={})",
            sign_config.room_id
        )));

        let mut request = sign_config.ws_url.clone()
            .into_client_request()
            .context("Failed to build WebSocket request from signed URL")?;

        let headers = request.headers_mut();
        headers.insert("Origin", "https://live.douyin.com".parse().unwrap());
        headers.insert(
            "User-Agent",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36 Edg/140.0.0.0"
                .parse()
                .unwrap(),
        );
        headers.insert("Cookie", sign_config.cookies.parse().unwrap());

        let (mut ws_stream, response) = connect_async(request)
            .await
            .map_err(|e| {
                anyhow::anyhow!("Failed to connect to WebSocket: {:?}", e)
            })?;

        callback(MessageType::System(format!(
            "WebSocket 连接成功！状态: {:?}",
            response.status()
        )));

        self.send_heartbeat(&mut ws_stream).await?;

        // 心跳间隔：默认 10 秒，服务端下发 heartbeatDuration 后按服务端给的来
        let mut heartbeat_secs: u64 = 10;
        let mut state = FrameState {
            heartbeat_secs: None,
            need_ack: false,
            seq_id: 0,
            log_id: 0,
        };
        let mut msg_count = 0u64;

        loop {
            // 用 sleep 而不是 interval，方便随时按服务端要求调整间隔
            let sleep_for = Duration::from_secs(heartbeat_secs);
            tokio::select! {
                msg = ws_stream.next() => {
                    match msg {
                        Some(Ok(WsMessage::Binary(data))) => {
                            // 整帧共用一个时间戳，避免每条消息都读一次系统时钟
                            let ts = chrono::Utc::now().timestamp();
                            let mut count = 0usize;
                            self.handle_frame(&data, ts, &mut callback, &mut count, &mut state);
                            msg_count += count as u64;

                            // 服务端要求 ack 时回一帧
                            if self.send_ack && state.need_ack {
                                let ack = build_ack_frame(state.seq_id, state.log_id);
                                if ws_stream.send(WsMessage::Binary(ack.into())).await.is_ok() {
                                    state.need_ack = false;
                                }
                            }

                            // 服务端指定的心跳间隔（限制在 1~120 秒）
                            if let Some(hb) = state.heartbeat_secs {
                                heartbeat_secs = hb;
                                state.heartbeat_secs = None;
                            }
                        }
                        Some(Ok(WsMessage::Text(text))) => {
                            callback(MessageType::System(format!("[文本消息] {}", text)));
                        }
                        Some(Ok(WsMessage::Ping(_))) => {
                            callback(MessageType::System("[Ping]".to_string()));
                        }
                        Some(Ok(WsMessage::Pong(_))) => {
                            callback(MessageType::System("[Pong]".to_string()));
                        }
                        Some(Ok(WsMessage::Close(msg))) => {
                            callback(MessageType::System(format!("[Close] {:?}", msg)));
                            break;
                        }
                        Some(Err(e)) => {
                            callback(MessageType::System(format!("接收消息失败: {}", e)));
                            anyhow::bail!("接收消息失败: {}", e);
                        }
                        None => {
                            callback(MessageType::System("WebSocket 连接已关闭".to_string()));
                            anyhow::bail!("WebSocket 连接已关闭");
                        }
                        _ => {}
                    }
                }
                _ = time::sleep(sleep_for) => {
                    if let Err(e) = self.send_heartbeat(&mut ws_stream).await {
                        callback(MessageType::System(format!("心跳发送失败: {}", e)));
                        anyhow::bail!("心跳发送失败: {}", e);
                    }
                }
            }
        }

        Ok(msg_count)
    }

    /// 处理一帧数据。
    ///
    /// 帧结构：PushFrame { seqId=1, logId=2, payloadType=7, payload=8 }，
    /// payload 是 gzip 压缩的 Response { messagesList=1, heartbeatDuration=8, needAck=9 }。
    fn handle_frame<F>(
        &self,
        data: &[u8],
        timestamp: i64,
        callback: &mut F,
        count: &mut usize,
        state: &mut FrameState,
    ) where
        F: FnMut(MessageType),
    {
        let frame = parse_protobuf(data);
        state.seq_id = frame.varint(PUSHFRAME_SEQ_ID).unwrap_or(state.seq_id);
        state.log_id = frame.varint(PUSHFRAME_LOG_ID).unwrap_or(state.log_id);

        // payload 正常在 PushFrame 的第 8 个字段；若取不到则回退到扫描 gzip magic
        let payload: &[u8] = match frame.bytes(PUSHFRAME_PAYLOAD) {
            Some(p) => p,
            None => match data.windows(2).position(|w| w == GZIP_MAGIC) {
                Some(idx) => &data[idx..],
                // 既没有 payload 也不是 gzip：当作裸 Response 处理
                None => data,
            },
        };

        // payload 可能是 gzip 压缩的，也可能是明文 Response
        let decompressed: Vec<u8>;
        let response_bytes: &[u8] = if payload.starts_with(&GZIP_MAGIC) {
            let mut buf = Vec::with_capacity(16 * 1024);
            let mut decoder = GzDecoder::new(payload);
            if decoder.read_to_end(&mut buf).is_err() {
                return;
            }
            decompressed = buf;
            &decompressed
        } else {
            payload
        };

        let response = parse_protobuf(response_bytes);

        // 服务端指定的心跳间隔
        if let Some(hb) = response.varint(RESPONSE_HEARTBEAT_DURATION) {
            if hb > 0 {
                state.heartbeat_secs = Some(normalize_heartbeat(hb));
            }
        }
        // 服务端要求 ack
        if response.varint(RESPONSE_NEED_ACK).unwrap_or(0) == 1 {
            state.need_ack = true;
        }

        // messagesList：每条 Message { method=1, payload=2 }
        for msg_bytes in response.bytes_all(RESPONSE_MESSAGES) {
            let msg_fields = parse_protobuf(msg_bytes);
            let method = match msg_fields.bytes(1).and_then(decode_string) {
                Some(m) => m,
                None => continue,
            };
            let payload = msg_fields.bytes(2).unwrap_or(&[]);
            *count += 1;
            if let Some(msg_type) = self.process_message(method, payload, timestamp) {
                callback(msg_type);
            }
        }
    }

    async fn send_heartbeat(
        &self,
        ws_stream: &mut tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
    ) -> Result<()> {
        const HEARTBEAT: [u8; 4] = [0x3a, 0x02, 0x68, 0x62];
        let message = WsMessage::Binary(HEARTBEAT.to_vec().into());
        ws_stream.send(message).await.context("Failed to send heartbeat")?;
        Ok(())
    }

    fn process_message(&self, method: &str, payload: &[u8], timestamp: i64) -> Option<MessageType> {
        match method {
            "WebcastChatMessage" | "WebcastAudioChatMessage" => {
                let (user, content) = extract_chat_message(payload);
                if !content.is_empty() {
                    Some(MessageType::Danmaku(crate::models::DanmakuMessage {
                        user_id: String::new(),
                        user_name: user,
                        content,
                        timestamp,
                        user_level: None,
                        user_medal: None,
                    }))
                } else {
                    None
                }
            }
            "WebcastMemberMessage" => {
                let (user, _count) = extract_member_message(payload);
                Some(MessageType::Enter(crate::models::EnterMessage {
                    user_id: String::new(),
                    user_name: user,
                    timestamp,
                }))
            }
            "WebcastRoomUserSeqMessage" => {
                let (online, total) = extract_room_user_seq(payload);
                let viewer_count = online.parse::<i64>().unwrap_or(total as i64);
                Some(MessageType::Stats(crate::models::RoomStats {
                    viewer_count,
                    like_count: 0,
                    gift_count: 0,
                    timestamp,
                }))
            }
            "WebcastGiftMessage" => {
                let (user, gift_name, gift_count) = extract_gift_message(payload);
                if !gift_name.is_empty() {
                    Some(MessageType::Gift(crate::models::GiftMessage {
                        user_id: String::new(),
                        user_name: user,
                        gift_name,
                        gift_count: gift_count as i64,
                        gift_price: None,
                        timestamp,
                    }))
                } else {
                    None
                }
            }
            "WebcastLikeMessage" => {
                let (user, like_count) = extract_like_message(payload);
                Some(MessageType::Like(crate::models::LikeMessage {
                    user_id: String::new(),
                    user_name: user,
                    like_count: like_count as i64,
                    timestamp,
                }))
            }
            "WebcastLotteryMessage" => {
                if let Some(desc) = extract_lottery_message(payload) {
                    if desc.contains("口令") || desc.contains("福袋") {
                        Some(MessageType::Lottery(desc))
                    } else {
                        None
                    }
                } else {
                    None
                }
            }
            "WebcastControlMessage" => {
                let fields = parse_protobuf(payload);
                if fields.varint(2) == Some(3) {
                    return Some(MessageType::System("直播已结束".to_string()));
                }
                None
            }
            "WebcastSocialMessage" => {
                let (user, action) = extract_social_message(payload);
                if !user.is_empty() {
                    Some(MessageType::Social(crate::models::SocialMessage {
                        user_name: user,
                        action,
                        timestamp,
                    }))
                } else {
                    None
                }
            }
            "WebcastFansclubMessage" => {
                let (user, club_name, level, action) = extract_fansclub_message(payload);
                if !user.is_empty() {
                    Some(MessageType::Fansclub(crate::models::FansclubMessage {
                        user_name: user,
                        club_name,
                        level,
                        action,
                        timestamp,
                    }))
                } else {
                    None
                }
            }
            // 以下三类消息更新频率高、信息密度低，默认不输出：
            // 直接跳过解析，避免为丢弃的消息白白付出 protobuf 解析开销。
            "WebcastRoomRankMessage" | "WebcastRoomStatsMessage" | "WebcastChatLikeMessage" => None,
            "WebcastHotChatMessage" => {
                let content = extract_hot_chat_message(payload);
                if !content.is_empty() {
                    Some(MessageType::HotChat(crate::models::HotChatMessage {
                        content,
                        timestamp,
                    }))
                } else {
                    None
                }
            }
            _ => None,
        }
    }
}

#[cfg(test)]
mod perf_tests {
    use super::*;
    use std::collections::HashMap;
    use std::time::Instant;

    // ---------- protobuf 编码辅助：构造一条合成的弹幕帧 ----------
    fn put_varint(out: &mut Vec<u8>, mut v: u64) {
        loop {
            let b = (v & 0x7f) as u8;
            v >>= 7;
            if v == 0 {
                out.push(b);
                break;
            }
            out.push(b | 0x80);
        }
    }

    fn put_bytes_field(out: &mut Vec<u8>, field: u32, data: &[u8]) {
        put_varint(out, ((field as u64) << 3) | 2);
        put_varint(out, data.len() as u64);
        out.extend_from_slice(data);
    }

    fn put_varint_field(out: &mut Vec<u8>, field: u32, v: u64) {
        put_varint(out, (field as u64) << 3);
        put_varint(out, v);
    }

    fn synthetic_frame() -> Vec<u8> {
        let mut user = Vec::new();
        put_varint_field(&mut user, 1, 7_314_298_754_668_572_38);
        put_bytes_field(&mut user, 3, "测试用户".as_bytes());

        let mut payload = Vec::new();
        put_bytes_field(&mut payload, 2, &user);
        put_bytes_field(&mut payload, 3, "这是一条测试弹幕内容".as_bytes());

        let mut frame = Vec::new();
        for method in [
            "WebcastChatMessage",
            "WebcastMemberMessage",
            "WebcastRoomUserSeqMessage",
            "WebcastLikeMessage",
            "WebcastChatMessage",
        ] {
            let mut msg = Vec::new();
            put_bytes_field(&mut msg, 1, method.as_bytes());
            put_bytes_field(&mut msg, 2, &payload);
            put_bytes_field(&mut frame, 5, &msg);
        }
        frame
    }

    // ---------- 旧实现（HashMap + 拷贝），仅作为对比基线 ----------
    enum LegacyValue {
        Varint(u64),
        Bytes(Vec<u8>),
    }

    fn legacy_parse(data: &[u8]) -> HashMap<u32, Vec<(u32, LegacyValue)>> {
        let mut fields: HashMap<u32, Vec<(u32, LegacyValue)>> = HashMap::new();
        let mut pos = 0;
        while pos < data.len() {
            let Some((tag, new_pos)) = read_varint(data, pos) else {
                break;
            };
            pos = new_pos;
            let field_number = (tag >> 3) as u32;
            let wire_type = (tag & 7) as u32;
            let value = match wire_type {
                0 => {
                    let Some((v, np)) = read_varint(data, pos) else {
                        break;
                    };
                    pos = np;
                    LegacyValue::Varint(v)
                }
                1 => {
                    if pos + 8 > data.len() {
                        break;
                    }
                    pos += 8;
                    continue;
                }
                2 => {
                    let Some((length, np)) = read_varint(data, pos) else {
                        break;
                    };
                    pos = np;
                    let len = length as usize;
                    if pos + len > data.len() {
                        break;
                    }
                    let val = LegacyValue::Bytes(data[pos..pos + len].to_vec());
                    pos += len;
                    val
                }
                5 => {
                    if pos + 4 > data.len() {
                        break;
                    }
                    pos += 4;
                    continue;
                }
                _ => break,
            };
            fields.entry(field_number).or_default().push((wire_type, value));
        }
        fields
    }

    fn legacy_first_bytes(
        fields: &HashMap<u32, Vec<(u32, LegacyValue)>>,
        field: u32,
    ) -> Option<Vec<u8>> {
        fields.get(&field).and_then(|entries| {
            entries.iter().find_map(|(wt, val)| match val {
                LegacyValue::Bytes(b) if *wt == 2 => Some(b.clone()),
                _ => None,
            })
        })
    }

    fn legacy_decode_string(data: &[u8]) -> Option<String> {
        let s = std::str::from_utf8(data).ok()?;
        if s.chars().all(|c| c >= ' ' || c == '\n' || c == '\r' || c == '\t') {
            Some(s.to_string())
        } else {
            None
        }
    }

    fn legacy_extract_user(user_bytes: &[u8]) -> (String, String) {
        let fields = legacy_parse(user_bytes);
        let user_id = fields
            .get(&1)
            .and_then(|e| {
                e.iter().find_map(|(wt, v)| match v {
                    LegacyValue::Varint(n) if *wt == 0 => Some(*n),
                    _ => None,
                })
            })
            .map(|v| v.to_string())
            .unwrap_or_default();
        let nick_name = legacy_first_bytes(&fields, 3)
            .and_then(|b| legacy_decode_string(&b))
            .unwrap_or_default();
        (nick_name, user_id)
    }

    fn legacy_extract_chat(payload: &[u8]) -> (String, String) {
        let fields = legacy_parse(payload);
        let content = legacy_first_bytes(&fields, 3)
            .and_then(|b| legacy_decode_string(&b))
            .unwrap_or_default();
        let (nick_name, user_id) = match legacy_first_bytes(&fields, 2) {
            Some(user_bytes) => legacy_extract_user(&user_bytes),
            None => (String::new(), String::new()),
        };
        let user = if !nick_name.is_empty() {
            nick_name
        } else if !user_id.is_empty() {
            user_id
        } else {
            "未知".to_string()
        };
        (user, content)
    }

    fn legacy_decode(frame: &[u8]) -> Vec<(String, String)> {
        let mut out = Vec::new();
        let push_fields = legacy_parse(frame);
        if let Some(entries) = push_fields.get(&5) {
            for (_, val) in entries {
                if let LegacyValue::Bytes(msg_bytes) = val {
                    let msg_fields = legacy_parse(msg_bytes);
                    let method = legacy_first_bytes(&msg_fields, 1)
                        .and_then(|b| legacy_decode_string(&b))
                        .unwrap_or_default();
                    let payload = legacy_first_bytes(&msg_fields, 2).unwrap_or_default();
                    if method == "WebcastChatMessage" {
                        out.push(legacy_extract_chat(&payload));
                    }
                }
            }
        }
        out
    }

    // ---------- 新实现（扁平字段表 + 零拷贝） ----------
    fn new_decode(frame: &[u8]) -> Vec<(String, String)> {
        let mut out = Vec::new();
        let fields = parse_protobuf(frame);
        for &(_, _, value) in fields.entries.iter().filter(|&&(f, _, _)| f == 5) {
            if let PbValue::Bytes(msg_bytes) = value {
                let msg_fields = parse_protobuf(msg_bytes);
                let method = msg_fields.bytes(1).and_then(decode_string).unwrap_or_default();
                let payload = msg_fields.bytes(2).unwrap_or(&[]);
                if method == "WebcastChatMessage" {
                    out.push(extract_chat_message(payload));
                }
            }
        }
        out
    }

    #[test]
    fn protobuf_zero_copy_matches_legacy_and_is_faster() {
        let frame = synthetic_frame();

        // 1) 正确性：两种实现结果必须一致
        assert_eq!(legacy_decode(&frame), new_decode(&frame));
        assert_eq!(new_decode(&frame).len(), 2);

        // 2) 性能对比（release 下数字更有意义）
        let iterations = 50_000u32;
        let t0 = Instant::now();
        for _ in 0..iterations {
            std::hint::black_box(legacy_decode(&frame));
        }
        let legacy = t0.elapsed();

        let t1 = Instant::now();
        for _ in 0..iterations {
            std::hint::black_box(new_decode(&frame));
        }
        let new = t1.elapsed();

        let legacy_fps = iterations as f64 / legacy.as_secs_f64();
        let new_fps = iterations as f64 / new.as_secs_f64();
        println!(
            "帧解析吞吐：旧实现 {:.0} 帧/s，新实现 {:.0} 帧/s，加速 {:.2}x（{} 轮）",
            legacy_fps,
            new_fps,
            new_fps / legacy_fps,
            iterations
        );

        if !cfg!(debug_assertions) {
            assert!(
                new <= legacy,
                "零拷贝解析不应慢于旧实现：new={:?} legacy={:?}",
                new,
                legacy
            );
        }
    }
}

#[cfg(test)]
mod frame_tests {
    use super::*;
    use flate2::write::GzEncoder;
    use flate2::Compression;
    use std::io::Write;

    fn put_varint(out: &mut Vec<u8>, mut v: u64) {
        loop {
            let b = (v & 0x7f) as u8;
            v >>= 7;
            if v == 0 {
                out.push(b);
                break;
            }
            out.push(b | 0x80);
        }
    }

    fn put_bytes_field(out: &mut Vec<u8>, field: u32, data: &[u8]) {
        put_varint(out, ((field as u64) << 3) | 2);
        put_varint(out, data.len() as u64);
        out.extend_from_slice(data);
    }

    fn put_varint_field(out: &mut Vec<u8>, field: u32, v: u64) {
        put_varint(out, (field as u64) << 3);
        put_varint(out, v);
    }

    /// 构造一个 Response：messagesList + heartbeatDuration + needAck
    fn build_response(heartbeat: u64, need_ack: bool) -> Vec<u8> {
        let mut user = Vec::new();
        put_varint_field(&mut user, 1, 998877);
        put_bytes_field(&mut user, 3, "帧测试用户".as_bytes());

        let mut payload = Vec::new();
        put_bytes_field(&mut payload, 2, &user);
        put_bytes_field(&mut payload, 3, "来自 PushFrame 的弹幕".as_bytes());

        let mut message = Vec::new();
        put_bytes_field(&mut message, 1, b"WebcastChatMessage");
        put_bytes_field(&mut message, 2, &payload);

        let mut response = Vec::new();
        put_bytes_field(&mut response, RESPONSE_MESSAGES, &message);
        put_varint_field(&mut response, RESPONSE_HEARTBEAT_DURATION, heartbeat);
        if need_ack {
            put_varint_field(&mut response, RESPONSE_NEED_ACK, 1);
        }
        response
    }

    /// 构造一个 PushFrame：payload 是 gzip 压缩后的 Response
    fn build_push_frame(response: &[u8], seq_id: u64, log_id: u64) -> Vec<u8> {
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(response).unwrap();
        let gz = encoder.finish().unwrap();

        let mut frame = Vec::new();
        put_varint_field(&mut frame, PUSHFRAME_SEQ_ID, seq_id);
        put_varint_field(&mut frame, PUSHFRAME_LOG_ID, log_id);
        put_bytes_field(&mut frame, PUSHFRAME_PAYLOAD_TYPE, b"gzip");
        put_bytes_field(&mut frame, PUSHFRAME_PAYLOAD, &gz);
        frame
    }

    #[test]
    fn frame_decoding_follows_pushframe_protocol() {
        let response = build_response(15, true);
        let frame = build_push_frame(&response, 42, 7);

        let client = DanmakuClient::new();
        let mut got: Vec<MessageType> = Vec::new();
        let mut count = 0usize;
        let mut state = FrameState {
            heartbeat_secs: None,
            need_ack: false,
            seq_id: 0,
            log_id: 0,
        };

        client.handle_frame(&frame, 100, &mut |m| got.push(m), &mut count, &mut state);

        assert_eq!(count, 1, "应解析出 1 条消息");
        assert_eq!(state.seq_id, 42);
        assert_eq!(state.log_id, 7);
        assert_eq!(state.heartbeat_secs, Some(15), "应读到服务端心跳间隔");
        assert!(state.need_ack, "应读到 needAck 标记");

        match &got[0] {
            MessageType::Danmaku(msg) => {
                assert_eq!(msg.content, "来自 PushFrame 的弹幕");
                assert_eq!(msg.user_name, "帧测试用户");
            }
            other => panic!("期望弹幕消息，实际 {:?}", other),
        }
    }

    #[test]
    fn ack_frame_carries_seq_and_type() {
        let ack = build_ack_frame(42, 7);
        let fields = parse_protobuf(&ack);
        assert_eq!(fields.varint(PUSHFRAME_SEQ_ID), Some(42));
        assert_eq!(fields.varint(PUSHFRAME_LOG_ID), Some(7));
        assert_eq!(
            fields.bytes(PUSHFRAME_PAYLOAD_TYPE),
            Some(&b"ack"[..])
        );
    }

    #[test]
    fn heartbeat_duration_is_clamped() {
        assert_eq!(normalize_heartbeat(10), 10);
        assert_eq!(normalize_heartbeat(0), 1);
        assert_eq!(normalize_heartbeat(500), 1); // 毫秒值换算后不足 1 秒，钳到 1 秒
        assert_eq!(normalize_heartbeat(1000), 1);
        assert_eq!(normalize_heartbeat(999999), 120); // 上限 120 秒
    }
}
