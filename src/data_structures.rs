use chrono::{DateTime, Utc};
use serde::de::{self, Unexpected, Visitor};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::fmt;

pub type EntryId = i64;
pub type MenuId = i64;

fn deserialize_flexible_i64<'de, D>(deserializer: D) -> Result<i64, D::Error>
where
    D: Deserializer<'de>,
{
    struct FlexibleI64Visitor;
    impl<'de> Visitor<'de> for FlexibleI64Visitor {
        type Value = i64;
        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            formatter.write_str("an integer, null, or a string representing an integer")
        }

        fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            Ok(value)
        }

        fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            i64::try_from(value).map_err(|_| E::invalid_value(Unexpected::Unsigned(value), &self))
        }

        fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            value
                .parse::<i64>()
                .map_err(|_| E::invalid_value(Unexpected::Str(value), &self))
        }

        fn visit_f64<E>(self, value: f64) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            if value.fract() == 0.0 && value >= i64::MIN as f64 && value <= i64::MAX as f64 {
                Ok(value as i64)
            } else {
                Err(E::invalid_value(Unexpected::Float(value), &self))
            }
        }

        fn visit_unit<E>(self) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            Err(E::invalid_type(Unexpected::Unit, &self))
        }
    }

    deserializer.deserialize_any(FlexibleI64Visitor)
}

fn deserialize_optional_flexible_i64<'de, D>(deserializer: D) -> Result<Option<i64>, D::Error>
where
    D: Deserializer<'de>,
{
    struct OptionalFlexibleI64Visitor;

    impl<'de> Visitor<'de> for OptionalFlexibleI64Visitor {
        type Value = Option<i64>;

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            formatter
                .write_str("an integer, null, empty string, or a string representing an integer")
        }

        fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            Ok(Some(value))
        }

        fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            i64::try_from(value)
                .map(Some)
                .map_err(|_| E::invalid_value(Unexpected::Unsigned(value), &self))
        }

        fn visit_f64<E>(self, value: f64) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            if value.fract() == 0.0 && value >= i64::MIN as f64 && value <= i64::MAX as f64 {
                Ok(Some(value as i64))
            } else {
                Ok(None)
            }
        }

        fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            if value.is_empty() {
                Ok(None)
            } else {
                Ok(value.parse::<i64>().ok())
            }
        }

        fn visit_none<E>(self) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            Ok(None)
        }

        fn visit_unit<E>(self) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            Ok(None)
        }

        fn visit_bool<E>(self, _v: bool) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            Ok(None)
        }

        fn visit_map<A>(self, _map: A) -> Result<Self::Value, A::Error>
        where
            A: de::MapAccess<'de>,
        {
            Ok(None)
        }

        fn visit_seq<A>(self, _seq: A) -> Result<Self::Value, A::Error>
        where
            A: de::SeqAccess<'de>,
        {
            Ok(None)
        }

        fn visit_bytes<E>(self, _v: &[u8]) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            Ok(None)
        }

        fn visit_byte_buf<E>(self, _v: Vec<u8>) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            Ok(None)
        }
    }

    deserializer.deserialize_any(OptionalFlexibleI64Visitor)
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct RawFilterEntry {
    #[serde(default)]
    pub values: Vec<Value>,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct FilterValues {
    #[serde(flatten)]
    pub filters: HashMap<String, Vec<String>>,
}

fn deserialize_optional_string<'de, D>(deserializer: D) -> Result<Option<String>, D::Error>
where
    D: Deserializer<'de>,
{
    let v: Value = Deserialize::deserialize(deserializer)?;
    match v {
        Value::String(s) if !s.is_empty() => Ok(Some(s)),
        _ => Ok(None),
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ListItem {
    #[serde(deserialize_with = "deserialize_flexible_i64")]
    pub entry_page_id: EntryId,
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub icon_url: String,
    #[serde(default, deserialize_with = "deserialize_optional_string")]
    pub desc: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub display_field: Option<Value>,
    #[serde(default)]
    pub filter_values: HashMap<String, RawFilterEntry>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct ListResponseData {
    #[serde(default, deserialize_with = "deserialize_optional_flexible_i64")]
    pub total: Option<i64>,
    #[serde(default = "default_value_null")]
    pub list: Value,
}

fn default_value_null() -> Value {
    Value::Null
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct BulkEntryPage {
    #[serde(deserialize_with = "deserialize_flexible_i64")]
    pub id: EntryId,
    #[serde(default, deserialize_with = "deserialize_optional_string")]
    pub name: Option<String>,
    #[serde(default, deserialize_with = "deserialize_optional_string")]
    pub desc: Option<String>,
    #[serde(default, deserialize_with = "deserialize_optional_string")]
    pub icon_url: Option<String>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct BulkResponseData {
    #[serde(default)]
    pub entry_pages: Vec<BulkEntryPage>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Component {
    pub component_id: String,
    #[serde(default)]
    pub data: Value,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Module {
    pub id: String,
    #[serde(default, deserialize_with = "deserialize_optional_string")]
    pub name: Option<String>,
    #[serde(default)]
    pub components: Vec<Component>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RawDetailPage {
    #[serde(deserialize_with = "deserialize_flexible_i64")]
    pub id: EntryId,
    #[serde(default)]
    pub name: String,
    #[serde(default, deserialize_with = "deserialize_optional_string")]
    pub desc: Option<String>,
    #[serde(default, deserialize_with = "deserialize_optional_string")]
    pub icon_url: Option<String>,
    #[serde(default, deserialize_with = "deserialize_optional_string")]
    pub header_img_url: Option<String>,
    #[serde(default)]
    pub modules: Vec<Module>,
    #[serde(default)]
    pub filter_values: Value,
    #[serde(deserialize_with = "deserialize_flexible_i64")]
    pub menu_id: MenuId,
    #[serde(default, deserialize_with = "deserialize_optional_string")]
    pub menu_name: Option<String>,
    #[serde(default, deserialize_with = "deserialize_optional_flexible_i64")]
    pub version: Option<i64>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct OutputDetailPage {
    pub id: EntryId,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub desc: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub icon_url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub header_img_url: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub modules: Vec<Module>,
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub filter_values: HashMap<String, Vec<String>>,
    pub menu_id: MenuId,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub menu_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<i64>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct DetailResponseData {
    pub page: RawDetailPage,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct NavMenuItem {
    pub menu_id: MenuId,
    pub name: String,
    #[serde(default)]
    pub icon_url: String,
}

#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "snake_case")]
pub struct RawNavEntry {
    pub menu: Option<RawNavMenu>,
    pub name: Option<String>,
    pub icon_url: Option<String>,
}

#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "snake_case")]
pub struct RawNavMenu {
    #[serde(deserialize_with = "deserialize_flexible_i64")]
    pub menu_id: MenuId,
}

#[derive(Deserialize, Debug, Clone)]
pub struct NavResponseData {
    #[serde(default)]
    pub nav: Vec<RawNavEntry>,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct CalendarAbstract {
    #[serde(deserialize_with = "deserialize_flexible_i64")]
    pub entry_page_id: EntryId,
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub icon_url: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub character_vision: Option<String>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct CalendarResponseData {
    #[serde(default)]
    pub calendar: Vec<Value>,
    #[serde(default)]
    pub op: Vec<Value>,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct OutputCalendarItem {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub drop_day: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub break_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub obtain_method: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub character_abstracts: Vec<CalendarAbstract>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub material_abstracts: Vec<CalendarAbstract>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub ep_abstracts: Vec<CalendarAbstract>,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct OutputOpItem {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub is_birth: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub ep_abstracts: Vec<CalendarAbstract>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub start_time: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub end_time: Option<String>,
}

#[derive(Deserialize, Debug)]
pub struct ApiResponseWrapper {
    #[serde(deserialize_with = "deserialize_flexible_i64")]
    pub retcode: i64,
    #[serde(default)]
    pub message: String,
    pub data: Option<Value>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ProcessedListItem {
    pub entry_page_id: EntryId,
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub icon_url: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub desc: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub display_field: Option<Value>,
    #[serde(default)]
    pub filter_values: FilterValues,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct OutputListFile {
    #[serde(with = "chrono::serde::ts_seconds")]
    pub version: DateTime<Utc>,
    pub language: String,
    pub menu_id: MenuId,
    pub menu_name: String,
    pub total_items: usize,
    pub list: Vec<ProcessedListItem>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct OutputCalendarFile {
    #[serde(with = "chrono::serde::ts_seconds")]
    pub version: DateTime<Utc>,
    pub language: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub calendar: Vec<OutputCalendarItem>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub op: Vec<OutputOpItem>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(tag = "type")]
pub enum HtmlNode {
    #[serde(rename = "text")]
    Text {
        text: String,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        styles: Vec<String>,
    },
    #[serde(rename = "paragraph")]
    Paragraph {
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        content: Vec<HtmlNode>,
        #[serde(skip_serializing_if = "Option::is_none")]
        alignment: Option<String>,
    },
    #[serde(rename = "heading")]
    Heading {
        level: u8,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        content: Vec<HtmlNode>,
        #[serde(skip_serializing_if = "Option::is_none")]
        alignment: Option<String>,
    },
    #[serde(rename = "list")]
    List {
        ordered: bool,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        items: Vec<Vec<HtmlNode>>,
    },
    #[serde(rename = "custom_entry")]
    CustomEntry {
        #[serde(deserialize_with = "deserialize_flexible_i64")]
        ep_id: EntryId,
        #[serde(default)]
        name: String,
        #[serde(default)]
        icon_url: String,
        #[serde(default, deserialize_with = "deserialize_flexible_i64")]
        amount: i64,
        #[serde(default = "default_display_style")]
        display_style: String,
        #[serde(
            default,
            skip_serializing_if = "Option::is_none",
            deserialize_with = "deserialize_optional_flexible_i64"
        )]
        menu_id: Option<MenuId>,
    },
    #[serde(rename = "custom_image")]
    CustomImage {
        url: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        alignment: Option<String>,
    },
}

fn default_display_style() -> String {
    "link".to_string()
}

impl HtmlNode {
    pub fn is_empty_text(&self) -> bool {
        matches!(self, HtmlNode::Text { text, .. } if text.trim().is_empty())
    }
}
