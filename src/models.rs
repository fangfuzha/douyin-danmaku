use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct RoomInfo {
    pub room_id: String,
    pub room_title: String,
    pub host_name: String,
    pub host_id: String,
    pub viewer_count: Option<i64>,
    pub like_count: Option<i64>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DanmakuMessage {
    pub user_id: String,
    pub user_name: String,
    pub content: String,
    pub timestamp: i64,
    pub user_level: Option<i32>,
    pub user_medal: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct GiftMessage {
    pub user_id: String,
    pub user_name: String,
    pub gift_name: String,
    pub gift_count: i64,
    pub gift_price: Option<f64>,
    pub timestamp: i64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct LikeMessage {
    pub user_id: String,
    pub user_name: String,
    pub like_count: i64,
    pub timestamp: i64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct EnterMessage {
    pub user_id: String,
    pub user_name: String,
    pub timestamp: i64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RoomStats {
    pub viewer_count: i64,
    pub like_count: i64,
    pub gift_count: i64,
    pub timestamp: i64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SocialMessage {
    pub user_name: String,
    pub action: String,
    pub timestamp: i64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct FansclubMessage {
    pub user_name: String,
    pub club_name: String,
    pub level: i32,
    pub action: String,
    pub timestamp: i64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RoomRankMessage {
    pub rank_text: String,
    pub timestamp: i64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RoomStatsMessage {
    pub display_short: String,
    pub display_middle: String,
    pub display_long: String,
    pub timestamp: i64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ChatLikeMessage {
    pub user_name: String,
    pub like_count: i64,
    pub timestamp: i64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct HotChatMessage {
    pub content: String,
    pub timestamp: i64,
}

#[derive(Debug)]
pub enum MessageType {
    Danmaku(DanmakuMessage),
    Gift(GiftMessage),
    Like(LikeMessage),
    Enter(EnterMessage),
    Stats(RoomStats),
    Lottery(String),
    Social(SocialMessage),
    Fansclub(FansclubMessage),
    RoomRank(RoomRankMessage),
    RoomStatsDetail(RoomStatsMessage),
    ChatLike(ChatLikeMessage),
    HotChat(HotChatMessage),
    Unknown(Vec<u8>),
}

impl std::fmt::Display for MessageType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MessageType::Danmaku(msg) => {
                write!(f, "[弹幕] {}: {}", msg.user_name, msg.content)
            }
            MessageType::Gift(msg) => {
                write!(f, "[礼物] {} 送出了 {} x{}", msg.user_name, msg.gift_name, msg.gift_count)
            }
            MessageType::Like(msg) => {
                write!(f, "[点赞] {} 点赞了 {}", msg.user_name, msg.like_count)
            }
            MessageType::Enter(msg) => {
                write!(f, "[进入] {} 进入了直播间", msg.user_name)
            }
            MessageType::Stats(stats) => {
                write!(f, "[统计] 观众: {}, 点赞: {}, 礼物: {}", 
                    stats.viewer_count, stats.like_count, stats.gift_count)
            }
            MessageType::Lottery(desc) => {
                write!(f, "[福袋口令] {}", desc)
            }
            MessageType::Social(msg) => {
                write!(f, "[关注] {} {}", msg.user_name, msg.action)
            }
            MessageType::Fansclub(msg) => {
                write!(f, "[粉丝团] {} {} {}级", msg.user_name, msg.club_name, msg.level)
            }
            MessageType::RoomRank(msg) => {
                write!(f, "[排行] {}", msg.rank_text)
            }
            MessageType::RoomStatsDetail(msg) => {
                write!(f, "[房间统计] {} | {} | {}", msg.display_short, msg.display_middle, msg.display_long)
            }
            MessageType::ChatLike(msg) => {
                write!(f, "[弹幕点赞] {} 点赞了 {}", msg.user_name, msg.like_count)
            }
            MessageType::HotChat(msg) => {
                write!(f, "[热门弹幕] {}", msg.content)
            }
            MessageType::Unknown(_) => {
                write!(f, "[未知] 未知消息类型")
            }
        }
    }
}