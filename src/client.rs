use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use tokio::time::{self, Duration};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message as WsMessage};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use flate2::read::GzDecoder;
use std::io::Read;

use crate::config::WsSignConfig;
use crate::models::MessageType;

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

/// 解码为 UTF-8 字符串（借用），仅接受可见字符。
fn decode_string(data: &[u8]) -> Option<&str> {
    let s = std::str::from_utf8(data).ok()?;
    if s.chars().all(|c| c >= ' ' || c == '\n' || c == '\r' || c == '\t') {
        Some(s)
    } else {
        None
    }
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

pub struct DanmakuClient;

impl DanmakuClient {
    pub fn new() -> Self {
        DanmakuClient
    }

    /// 连接 WebSocket 并持续回调消息，返回接收到的消息条数。
    pub async fn connect_with_sign<F>(&self, sign_config: &WsSignConfig, mut callback: F) -> Result<u64>
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

        let mut heartbeat_interval = time::interval(Duration::from_secs(10));
        let mut msg_count = 0u64;

        loop {
            tokio::select! {
                msg = ws_stream.next() => {
                    match msg {
                        Some(Ok(WsMessage::Binary(data))) => {
                            // 整帧共用一个时间戳，避免每条消息都读一次系统时钟
                            let ts = chrono::Utc::now().timestamp();
                            let mut count = 0usize;
                            self.handle_frame(&data, ts, &mut callback, &mut count);
                            msg_count += count as u64;
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
                            println!("[Close] {:?}", msg);
                            break;
                        }
                        Some(Err(e)) => {
                            eprintln!("接收消息失败: {}", e);
                            break;
                        }
                        None => {
                            eprintln!("WebSocket 连接已关闭");
                            break;
                        }
                        _ => {}
                    }
                }
                _ = heartbeat_interval.tick() => {
                    if let Err(e) = self.send_heartbeat(&mut ws_stream).await {
                        eprintln!("心跳发送失败: {}", e);
                        break;
                    }
                }
            }
        }

        Ok(msg_count)
    }

    /// 处理一帧数据：先处理 push 消息，再处理可能存在的 gzip（ack）消息。
    /// 全程借用帧内缓冲，不复制负载。
    fn handle_frame<F>(
        &self,
        data: &[u8],
        timestamp: i64,
        callback: &mut F,
        count: &mut usize,
    ) where
        F: FnMut(MessageType),
    {
        let push_fields = parse_protobuf(data);

        for &(_, _, value) in push_fields.entries.iter().filter(|&&(f, _, _)| f == 5) {
                if let PbValue::Bytes(msg_bytes) = value {
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

        // gzip（ack）负载：整帧扫描 magic，找到后解压并复用同一套解析逻辑
        const MAGIC: [u8; 2] = [0x1f, 0x8b];
        if let Some(idx) = data.windows(2).position(|w| w == MAGIC) {
            let mut decompressed = Vec::with_capacity(16 * 1024);
            let mut decoder = GzDecoder::new(&data[idx..]);
            if decoder.read_to_end(&mut decompressed).is_ok() {
                let resp_fields = parse_protobuf(&decompressed);
                for &(_, _, value) in resp_fields.entries.iter().filter(|&&(f, _, _)| f == 1) {
                    if let PbValue::Bytes(msg_bytes) = value {
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
