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

#[derive(Debug, Clone)]
enum PbValue {
    Varint(u64),
    Bytes(Vec<u8>),
    Fixed64(()),
    Fixed32(()),
}

fn parse_protobuf(data: &[u8]) -> std::collections::HashMap<u32, Vec<(u32, PbValue)>> {
    let mut fields: std::collections::HashMap<u32, Vec<(u32, PbValue)>> = std::collections::HashMap::new();
    let mut pos = 0;
    while pos < data.len() {
        let (tag, new_pos) = match read_varint(data, pos) {
            Some(v) => v,
            None => break,
        };
        pos = new_pos;
        let field_number = (tag >> 3) as u32;
        let wire_type = (tag & 7) as u32;

        let value = match wire_type {
            0 => {
                match read_varint(data, pos) {
                    Some((v, np)) => {
                        pos = np;
                        PbValue::Varint(v)
                    }
                    None => break,
                }
            }
            1 => {
                if pos + 8 > data.len() {
                    break;
                }
                pos += 8;
                PbValue::Fixed64(())
            }
            2 => {
                let (length, np) = match read_varint(data, pos) {
                    Some(v) => v,
                    None => break,
                };
                pos = np;
                let len = length as usize;
                if pos + len > data.len() {
                    break;
                }
                let val = PbValue::Bytes(data[pos..pos + len].to_vec());
                pos += len;
                val
            }
            5 => {
                if pos + 4 > data.len() {
                    break;
                }
                pos += 4;
                PbValue::Fixed32(())
            }
            _ => break,
        };

        fields.entry(field_number).or_insert_with(Vec::new).push((wire_type, value));
    }
    fields
}

fn try_decode_string(data: &[u8]) -> Option<String> {
    match std::str::from_utf8(data) {
        Ok(s) => {
            if s.chars().all(|c| c >= ' ' || c == '\n' || c == '\r' || c == '\t') {
                Some(s.to_string())
            } else {
                None
            }
        }
        Err(_) => None,
    }
}

fn get_first_bytes(fields: &std::collections::HashMap<u32, Vec<(u32, PbValue)>>, field: u32) -> Option<Vec<u8>> {
    fields.get(&field).and_then(|entries| {
        entries.iter().find_map(|(wt, val)| {
            if *wt == 2 {
                if let PbValue::Bytes(b) = val {
                    Some(b.clone())
                } else {
                    None
                }
            } else {
                None
            }
        })
    })
}

fn get_first_varint(fields: &std::collections::HashMap<u32, Vec<(u32, PbValue)>>, field: u32) -> Option<u64> {
    fields.get(&field).and_then(|entries| {
        entries.iter().find_map(|(wt, val)| {
            if *wt == 0 {
                if let PbValue::Varint(v) = val {
                    Some(*v)
                } else {
                    None
                }
            } else {
                None
            }
        })
    })
}

fn extract_user(user_bytes: &[u8]) -> (String, String) {
    let fields = parse_protobuf(user_bytes);
    let user_id = get_first_varint(&fields, 1).map(|v| v.to_string()).unwrap_or_default();
    let nick_name = get_first_bytes(&fields, 3)
        .and_then(|b| try_decode_string(&b))
        .unwrap_or_default();
    (nick_name, user_id)
}

fn extract_chat_message(payload: &[u8]) -> (String, String) {
    let fields = parse_protobuf(payload);
    let content = get_first_bytes(&fields, 3)
        .and_then(|b| try_decode_string(&b))
        .unwrap_or_default();

    let (nick_name, user_id) = if let Some(user_bytes) = get_first_bytes(&fields, 2) {
        extract_user(&user_bytes)
    } else {
        (String::new(), String::new())
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
    let member_count = get_first_varint(&fields, 3).unwrap_or(0);
    let (nick_name, _) = if let Some(user_bytes) = get_first_bytes(&fields, 2) {
        extract_user(&user_bytes)
    } else {
        (String::new(), String::new())
    };
    (nick_name, member_count)
}

fn extract_gift_message(payload: &[u8]) -> (String, String, u64) {
    let fields = parse_protobuf(payload);
    let (nick_name, _) = if let Some(user_bytes) = get_first_bytes(&fields, 2) {
        extract_user(&user_bytes)
    } else {
        (String::new(), String::new())
    };
    let gift_name = get_first_bytes(&fields, 3)
        .and_then(|b| try_decode_string(&b))
        .unwrap_or_default();
    let gift_count = get_first_varint(&fields, 4).unwrap_or(1);
    (nick_name, gift_name, gift_count)
}

fn extract_like_message(payload: &[u8]) -> (String, u64) {
    let fields = parse_protobuf(payload);
    let (nick_name, _) = if let Some(user_bytes) = get_first_bytes(&fields, 2) {
        extract_user(&user_bytes)
    } else {
        (String::new(), String::new())
    };
    let like_count = get_first_varint(&fields, 3).unwrap_or(1);
    (nick_name, like_count)
}

fn extract_lottery_message(payload: &[u8]) -> Option<String> {
    let fields = parse_protobuf(payload);
    if let Some(common_bytes) = get_first_bytes(&fields, 1) {
        let common_fields = parse_protobuf(&common_bytes);
        if let Some(status) = get_first_varint(&common_fields, 14) {
            if status == 2 {
                if let Some(desc_bytes) = get_first_bytes(&fields, 3) {
                    if let Some(desc) = try_decode_string(&desc_bytes) {
                        return Some(desc);
                    }
                }
            }
        }
    }
    None
}

/// 解析社交消息（关注、分享等）
fn extract_social_message(payload: &[u8]) -> (String, String) {
    let fields = parse_protobuf(payload);
    let (nick_name, _) = if let Some(user_bytes) = get_first_bytes(&fields, 2) {
        extract_user(&user_bytes)
    } else {
        (String::new(), String::new())
    };
    // 字段 1 是 CommonTextMessage，其中字段 2 是内容
    let action = if let Some(common_bytes) = get_first_bytes(&fields, 1) {
        let common_fields = parse_protobuf(&common_bytes);
        get_first_bytes(&common_fields, 2)
            .and_then(|b| try_decode_string(&b))
            .unwrap_or_default()
    } else {
        String::new()
    };
    let action = if action.is_empty() { "分享了直播间".to_string() } else { action };
    (nick_name, action)
}

/// 解析粉丝团消息
fn extract_fansclub_message(payload: &[u8]) -> (String, String, i32, String) {
    let fields = parse_protobuf(payload);
    let (nick_name, _) = if let Some(user_bytes) = get_first_bytes(&fields, 2) {
        extract_user(&user_bytes)
    } else {
        (String::new(), String::new())
    };
    // 粉丝团名称和等级通常在嵌套消息中
    let club_name = if let Some(club_bytes) = get_first_bytes(&fields, 1) {
        let club_fields = parse_protobuf(&club_bytes);
        get_first_bytes(&club_fields, 1)
            .and_then(|b| try_decode_string(&b))
            .unwrap_or_default()
    } else {
        String::new()
    };
    let level = if let Some(club_bytes) = get_first_bytes(&fields, 1) {
        let club_fields = parse_protobuf(&club_bytes);
        get_first_varint(&club_fields, 2).map(|v| v as i32).unwrap_or(0)
    } else {
        0
    };
    let action = get_first_varint(&fields, 3)
        .map(|v| match v {
            1 => "加入了粉丝团".to_string(),
            2 => "升级了粉丝团".to_string(),
            _ => "粉丝团变动".to_string(),
        })
        .unwrap_or_else(|| "粉丝团变动".to_string());
    (nick_name, club_name, level, action)
}

/// 解析房间排行消息
fn extract_room_rank_message(payload: &[u8]) -> String {
    let fields = parse_protobuf(payload);
    let count = fields.get(&2).map(|v| v.len()).unwrap_or(0)
        + fields.get(&3).map(|v| v.len()).unwrap_or(0);
    if count > 0 {
        format!("排行榜更新（共{}位上榜）", count)
    } else {
        String::new()
    }
}

/// 解析房间统计详情消息
fn extract_room_stats_detail(payload: &[u8]) -> (String, String, String) {
    let fields = parse_protobuf(payload);
    let display_short = get_first_bytes(&fields, 2)
        .and_then(|b| try_decode_string(&b))
        .unwrap_or_default();
    let display_middle = get_first_bytes(&fields, 3)
        .and_then(|b| try_decode_string(&b))
        .unwrap_or_default();
    let display_long = get_first_bytes(&fields, 4)
        .and_then(|b| try_decode_string(&b))
        .unwrap_or_default();
    (display_short, display_middle, display_long)
}

/// 解析弹幕点赞消息
fn extract_chat_like_message(payload: &[u8]) -> (String, i64) {
    let fields = parse_protobuf(payload);
    let (nick_name, _) = if let Some(user_bytes) = get_first_bytes(&fields, 2) {
        extract_user(&user_bytes)
    } else {
        (String::new(), String::new())
    };
    let like_count = get_first_varint(&fields, 3).unwrap_or(1) as i64;
    (nick_name, like_count)
}

/// 解析热门弹幕消息
fn extract_hot_chat_message(payload: &[u8]) -> String {
    let fields = parse_protobuf(payload);
    // 尝试从嵌套结构中提取内容
    for field_num in [1, 2, 3] {
        if let Some(b) = get_first_bytes(&fields, field_num) {
            if let Some(s) = try_decode_string(&b) {
                if !s.is_empty() {
                    return s;
                }
            }
            // 尝试嵌套解析
            let sub_fields = parse_protobuf(&b);
            for sub_field in [1, 2, 3] {
                if let Some(sb) = get_first_bytes(&sub_fields, sub_field) {
                    if let Some(s) = try_decode_string(&sb) {
                        if !s.is_empty() {
                            return s;
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
    let online = get_first_bytes(&fields, 10)
        .and_then(|b| try_decode_string(&b))
        .unwrap_or_else(|| "0".to_string());
    let total = get_first_varint(&fields, 3).unwrap_or(0);
    (online, total)
}

fn extract_method_payload(msg_bytes: &[u8]) -> Option<(String, Vec<u8>)> {
    let msg_fields = parse_protobuf(msg_bytes);
    let method = get_first_bytes(&msg_fields, 1)
        .and_then(|b| try_decode_string(&b))
        .unwrap_or_default();
    let payload = get_first_bytes(&msg_fields, 2).unwrap_or_default();
    if method.is_empty() {
        None
    } else {
        Some((method, payload))
    }
}

fn decode_ws_frame(data: &[u8]) -> Vec<(String, Vec<u8>)> {
    let mut messages = Vec::new();

    let push_fields = parse_protobuf(data);

    if let Some(entries) = push_fields.get(&5) {
        for (_, val) in entries {
            if let PbValue::Bytes(msg_bytes) = val {
                if let Some(mp) = extract_method_payload(msg_bytes) {
                    messages.push(mp);
                }
            }
        }
    }

    let magic_bytes = [0x1f, 0x8b];
    if let Some(idx) = data.windows(2).position(|w| w == magic_bytes) {
        let raw = &data[idx..];
        let mut decoder = GzDecoder::new(raw);
        let mut decompressed = Vec::new();
        if decoder.read_to_end(&mut decompressed).is_ok() {
            let resp_fields = parse_protobuf(&decompressed);
            if let Some(entries) = resp_fields.get(&1) {
                for (_, val) in entries {
                    if let PbValue::Bytes(msg_bytes) = val {
                        if let Some(mp) = extract_method_payload(msg_bytes) {
                            messages.push(mp);
                        }
                    }
                }
            }
        }
    }

    messages
}

pub struct DanmakuClient;

impl DanmakuClient {
    pub fn new() -> Self {
        DanmakuClient
    }

    pub async fn connect_with_sign<F>(&self, sign_config: &WsSignConfig, mut callback: F) -> Result<()>
    where
        F: FnMut(MessageType) + Send + 'static,
    {
        println!("使用真实签名直连 WebSocket");
        println!("room_id: {}", sign_config.room_id);

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

        println!("WebSocket 连接成功！状态: {:?}", response.status());

        self.send_heartbeat(&mut ws_stream).await?;

        let mut heartbeat_interval = time::interval(Duration::from_secs(10));
        let mut msg_count = 0u64;

        loop {
            tokio::select! {
                msg = ws_stream.next() => {
                    match msg {
                        Some(Ok(WsMessage::Binary(data))) => {
                            let messages = decode_ws_frame(&data);
                            for (method, payload) in messages {
                                msg_count += 1;
                                if let Some(msg_type) = self.process_message(&method, &payload) {
                                    callback(msg_type);
                                }
                            }
                        }
                        Some(Ok(WsMessage::Text(text))) => {
                            println!("[文本消息] {}", text);
                        }
                        Some(Ok(WsMessage::Ping(_))) => {
                            println!("[Ping]");
                        }
                        Some(Ok(WsMessage::Pong(_))) => {
                            println!("[Pong]");
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

        println!("共接收 {} 条消息", msg_count);
        Ok(())
    }

    async fn send_heartbeat(
        &self,
        ws_stream: &mut tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
    ) -> Result<()> {
        let heartbeat = [0x3a, 0x02, 0x68, 0x62];
        let message = WsMessage::Binary(heartbeat.to_vec().into());
        ws_stream.send(message).await.context("Failed to send heartbeat")?;
        Ok(())
    }

    fn process_message(&self, method: &str, payload: &[u8]) -> Option<MessageType> {
        match method {
            "WebcastChatMessage" | "WebcastAudioChatMessage" => {
                let (user, content) = extract_chat_message(payload);
                if !content.is_empty() {
                    Some(MessageType::Danmaku(crate::models::DanmakuMessage {
                        user_id: String::new(),
                        user_name: user,
                        content,
                        timestamp: chrono::Utc::now().timestamp(),
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
                    timestamp: chrono::Utc::now().timestamp(),
                }))
            }
            "WebcastRoomUserSeqMessage" => {
                let (online, total) = extract_room_user_seq(payload);
                let viewer_count = online.parse::<i64>().unwrap_or(total as i64);
                Some(MessageType::Stats(crate::models::RoomStats {
                    viewer_count,
                    like_count: 0,
                    gift_count: 0,
                    timestamp: chrono::Utc::now().timestamp(),
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
                        timestamp: chrono::Utc::now().timestamp(),
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
                    timestamp: chrono::Utc::now().timestamp(),
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
                if let Some(val) = get_first_varint(&fields, 2) {
                    if val == 3 {
                        println!("[系统消息] 直播已结束");
                    }
                }
                None
            }
            "WebcastSocialMessage" => {
                let (user, action) = extract_social_message(payload);
                if !user.is_empty() {
                    Some(MessageType::Social(crate::models::SocialMessage {
                        user_name: user,
                        action,
                        timestamp: chrono::Utc::now().timestamp(),
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
                        timestamp: chrono::Utc::now().timestamp(),
                    }))
                } else {
                    None
                }
            }
            "WebcastRoomRankMessage" => {
                // 排行榜更新频率高、信息密度低，默认不输出
                let _ = extract_room_rank_message(payload);
                None
            }
            "WebcastRoomStatsMessage" => {
                // 房间统计详情更新频率高、与 RoomUserSeqMessage 重复，默认不输出
                let _ = extract_room_stats_detail(payload);
                None
            }
            "WebcastChatLikeMessage" => {
                // 弹幕点赞刷屏严重，默认不输出
                let _ = extract_chat_like_message(payload);
                None
            }
            "WebcastHotChatMessage" => {
                let content = extract_hot_chat_message(payload);
                if !content.is_empty() {
                    Some(MessageType::HotChat(crate::models::HotChatMessage {
                        content,
                        timestamp: chrono::Utc::now().timestamp(),
                    }))
                } else {
                    None
                }
            }
            _ => {
                None
            }
        }
    }
}
