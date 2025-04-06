use once_cell::sync::Lazy;
use regex::Regex;
use reqwest::header::{
    HeaderMap, HeaderName, HeaderValue, ACCEPT, CONTENT_TYPE, ORIGIN, REFERER, USER_AGENT,
};
use std::collections::{HashMap, HashSet};

pub const BASE_API_URL: &str = "https://sg-wiki-api-static.hoyolab.com/hoyowiki/genshin/wapi";

pub static API_ENDPOINTS: Lazy<HashMap<&'static str, String>> = Lazy::new(|| {
    HashMap::from([
        ("nav", format!("{}/home/navigation", BASE_API_URL)),
        ("list", format!("{}/get_entry_page_list", BASE_API_URL)),
        ("detail", format!("{}/entry_page", BASE_API_URL)),
        ("bulk", format!("{}/entry_pages", BASE_API_URL)),
        ("calendar", format!("{}/home/calendar", BASE_API_URL)),
    ])
});

pub static SUPPORTED_LANGS: Lazy<Vec<String>> = Lazy::new(|| {
    let mut langs = vec![
        "id-id", "en-us", "zh-cn", "zh-tw", "de-de", "es-es", "fr-fr", "ja-jp", "ko-kr", "pt-pt",
        "ru-ru", "th-th", "vi-vn", "tr-tr", "it-it",
    ];
    langs.sort_unstable();
    langs.into_iter().map(String::from).collect()
});

pub const OUT_DIR: &str = "./";
pub const PAGE_SIZE: i64 = 30;
pub const MAX_LIST_CONCUR: usize = 20;
pub const MAX_DETAIL_CONCUR: usize = 1000;
pub const MAX_BULK_CONCUR: usize = 1000;
pub const HTTP_TIMEOUT_SECONDS: u64 = 30;
pub const HTTP_CONNECT_TIMEOUT: u64 = 15;
pub const MAX_RETRIES: u32 = 3;
pub const BULK_BATCH_SIZE: usize = 50;
pub const MAX_RECURSION_DEPTH: u32 = 500;

const ANDROID_VER: &str = "11";
const DEVICE_MODEL: &str = "Pixel 5";
const DEVICE_BRAND: &str = "Google";
const BUILD_ID: &str = "RQ3A.211001.001";
const CHROME_VER: &str = "107.0.0.0";
const WEBKIT_VER: &str = "537.36";
static USER_AGENT_VAL: Lazy<String> = Lazy::new(|| {
    format!("Mozilla/5.0 (Linux; Android {}; {} Build/{}; wv) AppleWebKit/{} (KHTML, like Gecko) Version/4.0 Chrome/{} Mobile Safari/{}", ANDROID_VER, DEVICE_MODEL, BUILD_ID, WEBKIT_VER, CHROME_VER, WEBKIT_VER)
});
const ORIGIN_VAL: &str = "https://www.hoyolab.com";
const REFERER_VAL: &str = "https://www.hoyolab.com/";

pub static BASE_UA_HEADERS: Lazy<HeaderMap> = Lazy::new(|| {
    let mut headers = HeaderMap::new();
    headers.insert(USER_AGENT, HeaderValue::from_static(&USER_AGENT_VAL));
    headers.insert(ORIGIN, HeaderValue::from_static(ORIGIN_VAL));
    headers.insert(REFERER, HeaderValue::from_static(REFERER_VAL));
    let device_name =
        HeaderValue::from_str(&format!("{}%20{}", DEVICE_BRAND, DEVICE_MODEL)).unwrap();
    headers.insert(HeaderName::from_static("x-rpc-device_name"), device_name);
    headers.insert(
        HeaderName::from_static("x-rpc-device_model"),
        HeaderValue::from_static(DEVICE_MODEL),
    );
    headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    headers.insert(
        ACCEPT,
        HeaderValue::from_static("application/json, text/plain, */*"),
    );
    headers
});

pub static LIST_FILTER_FIELDS: Lazy<HashSet<&'static str>> = Lazy::new(|| {
    HashSet::from([
        "character_vision",
        "character_region",
        "character_weapon",
        "character_property",
        "character_rarity",
        "weapon_type",
        "weapon_property",
        "weapon_rarity",
        "reliquary_effect",
        "object_type",
        "card_character_camp",
        "card_character_obtaining_method",
        "card_character_charging_point",
        "card_character_weapon_type",
        "card_character_element",
        "card_character_arkhe",
    ])
});

pub static MULTI_VALUE_FILTER_FIELDS: Lazy<HashSet<&'static str>> =
    Lazy::new(|| HashSet::from(["card_character_camp", "object_type", "reliquary_effect"]));

pub static INT_KEYS: Lazy<HashSet<&'static str>> = Lazy::new(|| {
    HashSet::from([
        "amount",
        "menu_id",
        "version",
        "entry_page_id",
        "menuId",
        "ep_id",
    ])
});
pub static HTML_CONTENT_COMPONENT_IDS: Lazy<HashSet<&'static str>> =
    Lazy::new(|| HashSet::from(["customize"]));
pub static HEADING_TAGS: Lazy<HashSet<&'static str>> =
    Lazy::new(|| HashSet::from(["h1", "h2", "h3", "h4", "h5", "h6"]));
pub static TARGET_HTML_CUSTOM_TAGS: Lazy<HashSet<&'static str>> = Lazy::new(|| {
    HashSet::from([
        "custom-entry",
        "custom-image",
        "custom-ruby",
        "custom-post",
        "custom-video",
    ])
});
pub static HTML_STRIP_TAGS: Lazy<HashSet<&'static str>> =
    Lazy::new(|| HashSet::from(["table", "tbody", "tr", "td", "th"]));
pub static HTML_BLOCK_TAGS: Lazy<HashSet<&'static str>> =
    Lazy::new(|| HashSet::from(["p", "div", "li", "ul", "ol", "blockquote", "figure"]));

pub static FORBIDDEN_CHARS_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r#"[<>:"/\\|?*\x00-\x1f\x7f^!@#$%^&*()+={}\[\];,.'’]"#).unwrap());
pub static WHITESPACE_RE: Lazy<Regex> = Lazy::new(|| Regex::new(r"[\s_]+").unwrap());
pub static NUM_STR_RE: Lazy<Regex> = Lazy::new(|| Regex::new(r"^-?\d+(\.\d+)?$").unwrap());
pub static JSON_LIKE_STR_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r#"^\s*([{\["]|\$\[)"#).unwrap());
pub static CONSECUTIVE_SLASH_RE: Lazy<Regex> = Lazy::new(|| Regex::new(r"//+").unwrap());
