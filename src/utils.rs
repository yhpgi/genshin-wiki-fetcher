use crate::constants::*;
use crate::data_structures::*;
use crate::errors::{AppError, AppResult};
use crate::logging::{log, LogLevel};
use scraper::{ElementRef, Html, Node, Selector};
use serde::Serialize;
use serde_json::{json, map::Map, Value};
use std::collections::{HashMap, HashSet};
use std::path::Path;
use tokio::fs::{self, File};
use tokio::io::AsyncWriteExt;

pub fn clean_filename<S: AsRef<str>>(name: S) -> String {
    let name_ref = name.as_ref().trim();
    let cleaned = FORBIDDEN_CHARS_RE.replace_all(name_ref, "_");
    let cleaned = WHITESPACE_RE.replace_all(&cleaned, "_");
    let cleaned = cleaned.trim_matches('_').to_lowercase();
    if cleaned.is_empty() {
        "invalid_name".to_string()
    } else {
        cleaned
    }
}

pub async fn run_blocking<F, T>(func: F) -> AppResult<T>
where
    F: FnOnce() -> AppResult<T> + Send + 'static,
    T: Send + 'static,
{
    match tokio::task::spawn_blocking(func).await {
        Ok(Ok(res)) => Ok(res),
        Ok(Err(e)) => Err(e),
        Err(e) => Err(AppError::JoinError(e)),
    }
}

async fn write_file_async(fpath: &Path, data: &[u8]) -> AppResult<()> {
    if let Some(parent) = fpath.parent() {
        fs::create_dir_all(parent).await?;
    }

    let mut file = File::create(fpath).await?;
    file.write_all(data).await?;
    file.sync_all().await?;
    Ok(())
}

pub async fn save_json<T>(fpath: &Path, data: T, log_ctx: &str) -> AppResult<bool>
where
    T: Serialize + Send + Sync + 'static,
{
    let prefix = if log_ctx.is_empty() {
        "Save JSON".to_string()
    } else {
        format!("Save JSON {}", log_ctx)
    };

    let result =
        run_blocking(move || serde_json::to_string(&data).map_err(AppError::SerdeJsonSerialize))
            .await;

    match result {
        Ok(json_string) => {
            let json_bytes = json_string.into_bytes();
            match write_file_async(fpath, &json_bytes).await {
                Ok(_) => Ok(true),
                Err(e) => {
                    log(
                        LogLevel::Error,
                        &format!(
                            "{} FAIL - Write Error: {}. File: '{}'",
                            prefix,
                            e,
                            fpath.display()
                        ),
                    );
                    if fs::try_exists(fpath).await.unwrap_or(false) {
                        let _ = fs::remove_file(fpath).await;
                    }

                    Ok(false)
                }
            }
        }

        Err(e) => {
            log(
                LogLevel::Error,
                &format!(
                    "{} FAIL - Encode Error: {}. File: '{}'",
                    prefix,
                    e,
                    fpath.display()
                ),
            );
            Ok(false)
        }
    }
}

fn parse_json_str_value(value: &str, depth: u32) -> AppResult<Value> {
    if depth > MAX_RECURSION_DEPTH {
        return Err(AppError::RecursionLimit {
            context: "parse_json_str_value".to_string(),
        });
    }

    let trimmed = value.trim();
    if !JSON_LIKE_STR_RE.is_match(trimmed) {
        return Ok(Value::String(value.to_string()));
    }

    let result = if trimmed.starts_with("$[") && trimmed.ends_with("]$") {
        let inner = trimmed[2..trimmed.len() - 2].trim();
        if inner.is_empty() {
            Ok(Value::Array(vec![]))
        } else {
            serde_json::from_str(inner)
        }
    } else {
        serde_json::from_str(trimmed)
    };

    match result {
        Ok(parsed_value) => normalize_value_types(parsed_value, depth),
        Err(_) => Ok(Value::String(value.to_string())),
    }
}

pub fn normalize_value_types(data: Value, depth: u32) -> AppResult<Value> {
    if depth > MAX_RECURSION_DEPTH {
        return Err(AppError::RecursionLimit {
            context: "normalize_value_types".to_string(),
        });
    }

    match data {
        Value::Object(map) => {
            let mut new_map = Map::with_capacity(map.len());
            for (k, v) in map {
                if k == "id" {
                    if !v.is_string() {
                        new_map.insert(k, v);
                    }

                    continue;
                }

                let final_v = normalize_value_types(v, depth + 1)?;

                let final_v_for_insert = if INT_KEYS.contains(k.as_str()) {
                    match &final_v {
                        Value::Number(n) => {
                            if let Some(i) = n.as_i64() {
                                Value::Number(serde_json::Number::from(i))
                            } else if let Some(f) = n.as_f64() {
                                if f.fract() == 0.0 && f >= i64::MIN as f64 && f <= i64::MAX as f64
                                {
                                    Value::Number(serde_json::Number::from(f as i64))
                                } else {
                                    final_v.clone()
                                }
                            } else {
                                final_v.clone()
                            }
                        }

                        Value::String(s) if NUM_STR_RE.is_match(s.trim()) => s
                            .trim()
                            .parse::<i64>()
                            .map(|i_val| Value::Number(serde_json::Number::from(i_val)))
                            .unwrap_or(final_v.clone()),
                        _ => final_v,
                    }
                } else if k == "data"
                    && !(final_v.is_object() || final_v.is_array() || final_v.is_null())
                {
                    json!({ "content": final_v })
                } else {
                    final_v
                };
                new_map.insert(k, final_v_for_insert);
            }

            Ok(Value::Object(new_map))
        }

        Value::Array(vec) => {
            let mut new_vec = Vec::with_capacity(vec.len());
            for item in vec {
                new_vec.push(normalize_value_types(item, depth + 1)?);
            }

            Ok(Value::Array(new_vec))
        }

        Value::String(s) => parse_json_str_value(&s, depth),
        _ => Ok(data),
    }
}

pub fn remove_ids_recursive(data: &mut Value) {
    match data {
        Value::Object(map) => {
            map.remove("id");
            for value in map.values_mut() {
                remove_ids_recursive(value);
            }
        }

        Value::Array(vec) => {
            for item in vec.iter_mut() {
                remove_ids_recursive(item);
            }
        }

        _ => {}
    }
}

pub fn collect_nested_ep_ids(
    data: &Value,
    collected_ids: &mut HashSet<EntryId>,
    depth: u32,
) -> AppResult<()> {
    if depth > MAX_RECURSION_DEPTH {
        return Err(AppError::RecursionLimit {
            context: "collect_nested_ep_ids".to_string(),
        });
    }

    match data {
        Value::Object(map) => {
            if let Some(id_val) = map.get("ep_id").or_else(|| map.get("entry_page_id")) {
                if let Some(id) = id_val.as_i64() {
                    if id > 0 {
                        collected_ids.insert(id);
                    }
                } else if let Some(id_str) = id_val.as_str() {
                    if let Ok(id) = id_str.parse() {
                        if id > 0 {
                            collected_ids.insert(id);
                        }
                    }
                }
            }

            if let Some(Value::String(node_type)) = map.get("type") {
                match node_type.as_str() {
                    "custom-entry" => {
                        if let Some(id_val) = map.get("ep_id") {
                            if let Some(id) = id_val.as_i64() {
                                if id > 0 {
                                    collected_ids.insert(id);
                                }
                            } else if let Some(id_str) = id_val.as_str() {
                                if let Ok(id) = id_str.parse() {
                                    if id > 0 {
                                        collected_ids.insert(id);
                                    }
                                }
                            }
                        }
                    }

                    "custom-post" => {
                        if let Some(id_val) = map.get("post_id") {
                            if let Some(id) = id_val.as_i64() {
                                if id > 0 {
                                    collected_ids.insert(id);
                                }
                            } else if let Some(id_str) = id_val.as_str() {
                                if let Ok(id) = id_str.parse() {
                                    if id > 0 {
                                        collected_ids.insert(id);
                                    }
                                }
                            }
                        }
                    }

                    _ => {}
                }
            }

            for v in map.values() {
                collect_nested_ep_ids(v, collected_ids, depth + 1)?;
            }
        }

        Value::Array(vec) => {
            for item in vec {
                collect_nested_ep_ids(item, collected_ids, depth + 1)?;
            }
        }

        _ => {}
    }

    Ok(())
}

pub fn update_value_with_bulk(
    data: &mut Value,
    primary_bulk: &HashMap<EntryId, Value>,
    fallback_bulk: &HashMap<String, HashMap<EntryId, Value>>,
    target_lang: &str,
    depth: u32,
) -> AppResult<bool> {
    if depth > MAX_RECURSION_DEPTH {
        return Err(AppError::RecursionLimit {
            context: "update_value_with_bulk".to_string(),
        });
    }

    let mut modified = false;

    match data {
        Value::Object(map) => {
            let current_id: Option<EntryId> = map
                .get("ep_id")
                .or_else(|| map.get("entry_page_id"))
                .or_else(|| map.get("id"))
                .and_then(|v| {
                    v.as_i64()
                        .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
                });

            let custom_entry_id: Option<EntryId> =
                if map.get("type") == Some(&Value::String("custom-entry".to_string())) {
                    map.get("ep_id").and_then(|v| {
                        v.as_i64()
                            .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
                    })
                } else {
                    None
                };

            let custom_post_id: Option<EntryId> =
                if map.get("type") == Some(&Value::String("custom-post".to_string())) {
                    map.get("post_id").and_then(|v| {
                        v.as_i64()
                            .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
                    })
                } else {
                    None
                };

            let id_to_use = current_id.or(custom_entry_id).or(custom_post_id);

            if let Some(id) = id_to_use {
                if id > 0 {
                    let primary_fetched = primary_bulk.get(&id).and_then(|v| v.as_object());
                    let mut final_icon_url: Option<Value> = None;

                    if let Some(bulk_name) = primary_fetched.and_then(|pf| pf.get("name")).cloned()
                    {
                        if map.get("name") != Some(&bulk_name) {
                            if map.get("type") != Some(&Value::String("custom-post".to_string()))
                                || !map.contains_key("name")
                            {
                                map.insert("name".to_string(), bulk_name);
                                modified = true;
                            }
                        }
                    }

                    let bulk_desc_opt = primary_fetched.and_then(|pf| pf.get("desc")).cloned();
                    if let Some(bulk_desc) = bulk_desc_opt {
                        if map.get("desc") != Some(&bulk_desc) {
                            map.insert("desc".to_string(), bulk_desc);
                            modified = true;
                        }
                    } else {
                        if map.contains_key("desc") {
                            map.remove("desc");
                            modified = true;
                        }
                    }

                    if let Some(icon_val) = primary_fetched.and_then(|pf| pf.get("icon_url")) {
                        if let Some(icon_str) = icon_val.as_str() {
                            if !icon_str.is_empty() && !icon_str.contains("invalid-file") {
                                final_icon_url = Some(icon_val.clone());
                            }
                        }
                    }

                    if final_icon_url.is_none() {
                        for (_lang, fallback_map) in fallback_bulk.iter() {
                            if let Some(fallback_entry) =
                                fallback_map.get(&id).and_then(|v| v.as_object())
                            {
                                if let Some(fallback_icon_val) = fallback_entry.get("icon_url") {
                                    if let Some(fallback_icon_str) = fallback_icon_val.as_str() {
                                        if !fallback_icon_str.is_empty()
                                            && !fallback_icon_str.contains("invalid-file")
                                        {
                                            final_icon_url = Some(fallback_icon_val.clone());
                                            break;
                                        }
                                    }
                                }
                            }
                        }
                    }

                    let current_icon = map.get("icon_url").or_else(|| map.get("icon"));
                    if let Some(icon_to_use) = final_icon_url {
                        if current_icon != Some(&icon_to_use) {
                            map.insert("icon_url".to_string(), icon_to_use);
                            map.remove("icon");
                            modified = true;
                        }
                    } else {
                        let empty_icon = json!("");
                        if current_icon.is_some() && current_icon != Some(&empty_icon) {
                            map.insert("icon_url".to_string(), empty_icon.clone());
                            map.remove("icon");
                            modified = true;
                        } else if current_icon.is_none() && !map.contains_key("icon_url") {
                            map.insert("icon_url".to_string(), empty_icon);
                            modified = true;
                        } else if current_icon.is_none() && map.get("icon_url") != Some(&empty_icon)
                        {
                            map.insert("icon_url".to_string(), empty_icon);
                            modified = true;
                        }
                    }
                }
            }

            let keys: Vec<String> = map.keys().cloned().collect();
            for key in keys {
                if id_to_use.is_some()
                    && (key == "name" || key == "desc" || key == "icon_url" || key == "icon")
                {
                } else if let Some(value) = map.get_mut(&key) {
                    if update_value_with_bulk(
                        value,
                        primary_bulk,
                        fallback_bulk,
                        target_lang,
                        depth + 1,
                    )? {
                        modified = true;
                    }
                }
            }
        }

        Value::Array(vec) => {
            for item in vec.iter_mut() {
                if update_value_with_bulk(
                    item,
                    primary_bulk,
                    fallback_bulk,
                    target_lang,
                    depth + 1,
                )? {
                    modified = true;
                }
            }
        }

        _ => {}
    }

    Ok(modified)
}

fn get_node_alignment(element: &ElementRef) -> Option<String> {
    element.value().attr("style").and_then(|style| {
        let lower = style.to_lowercase();
        if lower.contains("text-align: center") {
            Some("center".to_string())
        } else if lower.contains("text-align: right") {
            Some("right".to_string())
        } else if lower.contains("text-align: left") {
            Some("left".to_string())
        } else {
            None
        }
    })
}

pub async fn process_html_in_component_data(component: &mut Component) -> AppResult<()> {
    if HTML_CONTENT_COMPONENT_IDS.contains(component.component_id.as_str()) {
        let mut html_opt: Option<String> = None;
        let mut requires_conversion = false;

        match &component.data {
            Value::Object(map) => {
                html_opt = map
                    .get("content")
                    .or_else(|| map.get("data").and_then(|d| d.get("content")))
                    .and_then(Value::as_str)
                    .map(String::from);
            }

            Value::String(s) => {
                html_opt = Some(s.clone());
                requires_conversion = true;
            }

            _ => {}
        }

        let final_parsed_nodes = if let Some(html_content) = html_opt {
            if html_content.trim().is_empty() {
                vec![]
            } else {
                match run_blocking(move || parse_html_content(&html_content)).await {
                    Ok(parsed_nodes) => parsed_nodes,
                    Err(e) => {
                        log(
                            LogLevel::Error,
                            &format!(
                                "HTML Parsing Failed (Component: {}): {:?}",
                                component.component_id, e
                            ),
                        );
                        vec![HtmlNode::RichText {
                            text: "[HTML PARSE ERROR]".into(),
                            alignment: None,
                        }]
                    }
                }
            }
        } else {
            vec![]
        };

        let parsed_value =
            serde_json::to_value(final_parsed_nodes).map_err(AppError::SerdeJsonSerialize)?;
        if requires_conversion {
            component.data = json!({ "parsed_content": parsed_value });
        } else if let Value::Object(map) = &mut component.data {
            let target_map = if map.contains_key("data") && map["data"].is_object() {
                map["data"].as_object_mut().unwrap()
            } else {
                map
            };
            target_map.insert("parsed_content".to_string(), parsed_value);
            target_map.remove("content");
        } else {
            component.data = json!({ "parsed_content": parsed_value });
        }
    }

    Ok(())
}

fn filter_component_inplace(comp: &mut Component) -> bool {
    !comp.component_id.is_empty()
        && match &comp.data {
            Value::Null => false,
            Value::Object(map) => !map.is_empty(),
            Value::Array(arr) => !arr.is_empty(),
            _ => true,
        }
}

fn filter_module_inplace(module: &mut Module) -> bool {
    module.components.retain_mut(filter_component_inplace);
    module.name.is_some() || !module.components.is_empty()
}

pub async fn process_and_filter_detail(
    mut raw_page: RawDetailPage,
    page_id: EntryId,
    menu_id: MenuId,
    lang: &str,
) -> AppResult<Option<OutputDetailPage>> {
    let filter_values_processed = process_filters_value(&raw_page.filter_values);

    for module in &mut raw_page.modules {
        for component in &mut module.components {
            component.data = normalize_value_types(component.data.take(), 0)?;
            process_html_in_component_data(component).await?;
            remove_ids_recursive(&mut component.data);
        }
    }

    raw_page.modules.retain_mut(filter_module_inplace);

    let name_is_empty = raw_page.name.is_empty();
    let desc_is_none = raw_page.desc.is_none();
    let modules_is_empty = raw_page.modules.is_empty();
    let filters_is_null = filter_values_processed.is_null();

    let header_img_is_none = raw_page.header_img_url.is_none();
    let icon_url_is_none = raw_page.icon_url.is_none();

    let has_content = !name_is_empty
        || !desc_is_none
        || !modules_is_empty
        || !filters_is_null
        || !header_img_is_none
        || !icon_url_is_none;

    if !has_content {
        log(
            LogLevel::Info,
            &format!(
                "Detail Skip Entry:{} [{}] (No content after processing)",
                page_id, lang
            ),
        );
        return Ok(None);
    }

    Ok(Some(OutputDetailPage {
        id: page_id,
        name: Some(raw_page.name).filter(|s| !s.is_empty()),
        desc: raw_page.desc,
        icon_url: raw_page.icon_url,
        header_img_url: raw_page.header_img_url,
        modules: raw_page.modules,
        filter_values: filter_values_processed,
        menu_id,
        menu_name: raw_page.menu_name,
        version: raw_page.version,
    }))
}

pub fn process_filters_value(raw_filters_val: &Value) -> Value {
    let mut processed_map = Map::new();

    if let Value::Object(raw_filters) = raw_filters_val {
        for key_ref in LIST_FILTER_FIELDS.iter() {
            let key = *key_ref;
            if let Some(field_data) = raw_filters.get(key) {
                let mut values: HashSet<String> = HashSet::new();

                let values_array = field_data
                    .get("values")
                    .or_else(|| field_data.get("value"))
                    .or_else(|| {
                        field_data
                            .get("value_types")
                            .and_then(|vt| vt.get(0))
                            .and_then(|ftype| ftype.get("value"))
                    });

                if let Some(Value::Array(vals)) = values_array {
                    for v in vals {
                        match v {
                            Value::String(s) if !s.is_empty() => {
                                values.insert(s.clone());
                            }

                            Value::Number(n) => {
                                values.insert(n.to_string());
                            }

                            Value::Bool(b) => {
                                values.insert(b.to_string());
                            }

                            _ => {}
                        }
                    }
                } else if let Some(single_val) = values_array {
                    match single_val {
                        Value::String(s) if !s.is_empty() => {
                            values.insert(s.clone());
                        }

                        Value::Number(n) => {
                            values.insert(n.to_string());
                        }

                        Value::Bool(b) => {
                            values.insert(b.to_string());
                        }

                        _ => {}
                    }
                } else if let Value::String(s) = field_data {
                    if !s.is_empty() {
                        values.insert(s.clone());
                    }
                } else if let Value::Number(n) = field_data {
                    values.insert(n.to_string());
                } else if let Value::Bool(b) = field_data {
                    values.insert(b.to_string());
                }

                if !values.is_empty() {
                    let mut sorted: Vec<String> = values.into_iter().collect();
                    sorted.sort_unstable();

                    if MULTI_VALUE_FILTER_FIELDS.contains(key) {
                        processed_map.insert(
                            key.to_string(),
                            Value::Array(sorted.into_iter().map(Value::String).collect()),
                        );
                    } else {
                        processed_map.insert(key.to_string(), Value::String(sorted[0].clone()));
                    }
                }
            }
        }
    }

    if processed_map.is_empty() {
        Value::Null
    } else {
        Value::Object(processed_map)
    }
}

pub fn process_cal_abstracts_bulk(
    abstract_list_val: &Value,
    vision_map: &HashMap<EntryId, String>,
    _bulk_data: &HashMap<EntryId, Value>,
) -> Vec<CalendarAbstract> {
    let mut simplified = Vec::new();
    if let Value::Array(list) = abstract_list_val {
        for val in list {
            if let Value::Object(map) = val {
                let id_res = map
                    .get("entry_page_id")
                    .or_else(|| map.get("id"))
                    .and_then(|v| {
                        v.as_i64()
                            .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
                    });

                if let Some(id) = id_res {
                    if id > 0 {
                        let name = map.get("name").and_then(Value::as_str).unwrap_or("");
                        let icon = map
                            .get("icon_url")
                            .or_else(|| map.get("icon"))
                            .and_then(Value::as_str)
                            .unwrap_or("");
                        let desc = map.get("desc").and_then(Value::as_str).map(String::from);

                        if !name.is_empty() || !icon.is_empty() {
                            simplified.push(CalendarAbstract {
                                id,
                                name: name.to_string(),
                                icon_url: icon.to_string(),
                                desc,
                                character_vision: vision_map.get(&id).cloned(),
                            });
                        }
                    }
                }
            }
        }
    }

    simplified
}

pub fn format_cal_date(raw_date: Option<&Value>) -> Option<String> {
    raw_date.and_then(Value::as_str).and_then(|s| {
        let parts: Vec<&str> = s.split('-').collect();
        if parts.len() == 3 && parts[0].len() == 4 && parts[1].len() == 2 && parts[2].len() == 2 {
            Some(format!("{}-{}", parts[1], parts[2]))
        } else if parts.len() == 2 && parts[0].len() == 2 && parts[1].len() == 2 {
            Some(s.to_string())
        } else {
            None
        }
    })
}

fn clean_consecutive_slashes(text: &str) -> String {
    CONSECUTIVE_SLASH_RE.replace_all(text, "").into_owned()
}

fn parse_html_content(html_string: &str) -> AppResult<Vec<HtmlNode>> {
    let initial_cleaned = clean_consecutive_slashes(html_string.trim());
    if initial_cleaned.is_empty() {
        return Ok(vec![]);
    }

    let wrapped = if !initial_cleaned.to_lowercase().starts_with("<html")
        && !initial_cleaned.to_lowercase().starts_with("<body")
        && !initial_cleaned.to_lowercase().starts_with("<div")
        && !initial_cleaned.to_lowercase().starts_with("<p")
    {
        format!("<body>{}</body>", initial_cleaned)
    } else {
        initial_cleaned.to_string()
    };

    let fragment = Html::parse_fragment(&wrapped);
    let root_element = fragment.root_element();
    let parsed_nodes = parse_element_content(root_element, 0, false)?;
    Ok(merge_consecutive_rich_text(parsed_nodes))
}

fn parse_element_content(
    element_ref: ElementRef<'_>,
    depth: u32,
    is_ordered_list: bool,
) -> AppResult<Vec<HtmlNode>> {
    if depth > MAX_RECURSION_DEPTH / 2 {
        return Ok(vec![HtmlNode::RichText {
            text: "[HTML Depth Limit]".into(),
            alignment: None,
        }]);
    }

    let mut results = Vec::new();
    let mut current_rich_text = String::new();
    let mut list_counter = 1;

    for child_node in element_ref.children() {
        match child_node.value() {
            Node::Text(text_node) => {
                let text = clean_consecutive_slashes(text_node.text.as_ref());

                current_rich_text.push_str(&text);
            }

            Node::Element(el_data) => {
                if let Some(child_element_ref) = ElementRef::wrap(child_node) {
                    let tag_name = el_data.name().to_lowercase();
                    let current_alignment = get_node_alignment(&element_ref);

                    if HEADING_TAGS.contains(tag_name.as_str()) {
                        flush_rich_text(
                            &mut current_rich_text,
                            &mut results,
                            current_alignment.as_deref(),
                        );
                        if let Ok(level) = tag_name[1..].parse::<u8>() {
                            let alignment = get_node_alignment(&child_element_ref);
                            let heading_text = extract_plain_text(child_element_ref, depth + 1)?;
                            if !heading_text.is_empty() {
                                results.push(HtmlNode::Heading {
                                    level,
                                    text: heading_text,
                                    alignment,
                                });
                            }
                        }
                    } else if TARGET_HTML_CUSTOM_TAGS.contains(tag_name.as_str()) {
                        flush_rich_text(
                            &mut current_rich_text,
                            &mut results,
                            current_alignment.as_deref(),
                        );
                        if let Some(node) = process_custom_element(child_element_ref) {
                            results.push(node);
                        }
                    } else if tag_name == "br" {
                        if !current_rich_text.ends_with('\n') {
                            current_rich_text.push('\n');
                        }
                    } else if tag_name == "ul" || tag_name == "ol" {
                        flush_rich_text(
                            &mut current_rich_text,
                            &mut results,
                            current_alignment.as_deref(),
                        );

                        let new_is_ordered = tag_name == "ol";
                        results.extend(parse_element_content(
                            child_element_ref,
                            depth + 1,
                            new_is_ordered,
                        )?);

                        flush_rich_text(
                            &mut current_rich_text,
                            &mut results,
                            current_alignment.as_deref(),
                        );
                    } else if tag_name == "li" {
                        flush_rich_text(
                            &mut current_rich_text,
                            &mut results,
                            current_alignment.as_deref(),
                        );

                        let li_content_nodes =
                            parse_element_content(child_element_ref, depth + 1, is_ordered_list)?;
                        let prefix = if is_ordered_list {
                            format!("{}. ", list_counter)
                        } else {
                            "* ".to_string()
                        };
                        list_counter += 1;

                        if let Some(first_node) = li_content_nodes.first() {
                            match first_node {
                                HtmlNode::RichText { text, alignment } => {
                                    let mut prefixed_nodes = vec![HtmlNode::RichText {
                                        text: format!("{}{}", prefix, text),
                                        alignment: alignment.clone(),
                                    }];
                                    prefixed_nodes.extend_from_slice(&li_content_nodes[1..]);
                                    results.extend(prefixed_nodes);
                                }

                                _ => {
                                    results.push(HtmlNode::RichText {
                                        text: prefix,
                                        alignment: None,
                                    });
                                    results.extend(li_content_nodes);
                                }
                            }
                        } else {
                            results.push(HtmlNode::RichText {
                                text: prefix,
                                alignment: None,
                            });
                        }

                        if !results.is_empty() {
                            if let Some(last_node) = results.last_mut() {
                                match last_node {
                                    HtmlNode::RichText { text, .. } => {
                                        if !text.ends_with('\n') {
                                            text.push('\n');
                                        }
                                    }

                                    _ => {}
                                }
                            }
                        }
                    } else if HTML_STRIP_TAGS.contains(tag_name.as_str()) {
                        results.extend(parse_element_content(
                            child_element_ref,
                            depth + 1,
                            is_ordered_list,
                        )?);
                    } else if HTML_BLOCK_TAGS.contains(tag_name.as_str())
                        || tag_name == "body"
                        || tag_name == "html"
                    {
                        let block_alignment = get_node_alignment(&child_element_ref);
                        flush_rich_text(
                            &mut current_rich_text,
                            &mut results,
                            block_alignment.as_deref(),
                        );

                        results.extend(parse_element_content(
                            child_element_ref,
                            depth + 1,
                            is_ordered_list,
                        )?);
                        flush_rich_text(
                            &mut current_rich_text,
                            &mut results,
                            block_alignment.as_deref(),
                        );

                        if !results.is_empty() {
                            if let Some(last_node) = results.last_mut() {
                                match last_node {
                                    HtmlNode::RichText { text, .. } => {
                                        if !text.ends_with('\n') {
                                            text.push('\n');
                                        }
                                    }

                                    _ => {}
                                }
                            }
                        }
                    } else {
                        let inline_content = extract_plain_text(child_element_ref, depth + 1)?;
                        if !inline_content.is_empty() {
                            if !current_rich_text.is_empty()
                                && !current_rich_text.ends_with(char::is_whitespace)
                            {
                                current_rich_text.push(' ');
                            }

                            current_rich_text.push_str(&inline_content);
                        }
                    }
                }
            }

            _ => {}
        }
    }

    flush_rich_text(
        &mut current_rich_text,
        &mut results,
        get_node_alignment(&element_ref).as_deref(),
    );
    results.retain(|node| !node.is_empty_text());
    Ok(results)
}

fn merge_consecutive_rich_text(nodes: Vec<HtmlNode>) -> Vec<HtmlNode> {
    let mut merged = Vec::with_capacity(nodes.len());
    for node in nodes {
        match node {
            HtmlNode::RichText { text, alignment } => {
                let current_text = normalize_whitespace(&text);
                if current_text.is_empty() {
                    continue;
                }

                if let Some(HtmlNode::RichText {
                    text: last_text,
                    alignment: last_alignment,
                }) = merged.last_mut()
                {
                    if *last_alignment == alignment {
                        if !last_text.is_empty() && !last_text.ends_with('\n') {
                            last_text.push('\n');
                        }

                        last_text.push_str(&current_text);
                        continue;
                    }
                }

                merged.push(HtmlNode::RichText {
                    text: current_text,
                    alignment,
                });
            }

            other => merged.push(other),
        }
    }

    for node in merged.iter_mut() {
        if let HtmlNode::RichText { text, .. } = node {
            *text = text.trim().to_string();
        }
    }

    merged.retain(|node| match node {
        HtmlNode::RichText { text, .. } => !text.is_empty(),
        _ => true,
    });

    merged
}

fn flush_rich_text(text_buffer: &mut String, results: &mut Vec<HtmlNode>, alignment: Option<&str>) {
    let cleaned_text = clean_consecutive_slashes(text_buffer);

    if !cleaned_text.trim().is_empty() {
        results.push(HtmlNode::RichText {
            text: cleaned_text.clone(),
            alignment: alignment.map(String::from),
        });
    }

    text_buffer.clear();
}

fn normalize_whitespace(text: &str) -> String {
    text.split('\n')
        .map(|line| line.split_whitespace().collect::<Vec<&str>>().join(" "))
        .collect::<Vec<String>>()
        .join("\n")
        .trim()
        .to_string()
}

fn extract_plain_text(element_ref: ElementRef<'_>, depth: u32) -> AppResult<String> {
    if depth > MAX_RECURSION_DEPTH / 2 {
        return Ok(String::new());
    }

    let mut text_content = String::new();
    for child_node in element_ref.children() {
        match child_node.value() {
            Node::Text(text_node) => {
                text_content.push_str(text_node.text.as_ref());
            }

            Node::Element(el_data) => {
                let tag_name = el_data.name().to_lowercase();

                let needs_separator = HTML_BLOCK_TAGS.contains(tag_name.as_str())
                    || tag_name == "br"
                    || tag_name == "li";

                if needs_separator
                    && !text_content.is_empty()
                    && !text_content.ends_with(char::is_whitespace)
                {
                    text_content.push(' ');
                }

                if let Some(child_el_ref) = ElementRef::wrap(child_node) {
                    if tag_name != "style" && tag_name != "script" {
                        let child_text = extract_plain_text(child_el_ref, depth + 1)?;
                        if !child_text.is_empty() {
                            if !text_content.is_empty()
                                && !text_content.ends_with(char::is_whitespace)
                                && !child_text.starts_with(char::is_whitespace)
                            {
                                text_content.push(' ');
                            }

                            text_content.push_str(&child_text);
                        }
                    }
                }

                if needs_separator
                    && !text_content.is_empty()
                    && !text_content.ends_with(char::is_whitespace)
                {
                    text_content.push(' ');
                }
            }

            _ => {}
        }
    }

    Ok(normalize_whitespace(&clean_consecutive_slashes(
        &text_content,
    )))
}

fn process_custom_element(element_ref: ElementRef<'_>) -> Option<HtmlNode> {
    let el_val = element_ref.value();
    let tag_name = el_val.name().to_lowercase();

    match tag_name.as_str() {
        "custom-entry" => el_val
            .attr("epid")
            .and_then(|s| s.trim().parse().ok())
            .filter(|&id: &EntryId| id > 0)
            .map(|id| HtmlNode::CustomEntry {
                ep_id: id,
                name: clean_consecutive_slashes(el_val.attr("name").unwrap_or("").trim()),
                icon_url: el_val.attr("icon").unwrap_or("").trim().to_string(),
                amount: el_val
                    .attr("amount")
                    .and_then(|s| s.trim().parse().ok())
                    .unwrap_or(0),
                display_style: el_val
                    .attr("displaystyle")
                    .unwrap_or("link")
                    .trim()
                    .to_string(),
                menu_id: el_val.attr("menuid").and_then(|s| s.trim().parse().ok()),
            }),
        "custom-image" => el_val
            .attr("url")
            .map(|s| s.trim())
            .filter(|url| !url.is_empty())
            .map(|url| HtmlNode::CustomImage {
                url: url.to_string(),
                alignment: el_val
                    .attr("align")
                    .map(|s| s.trim().to_lowercase())
                    .filter(|s| !s.is_empty()),
            }),
        "custom-ruby" => {
            let rb_sel = Selector::parse("rb").ok()?;
            let rt_sel = Selector::parse("rt").ok()?;
            let rb_text = element_ref
                .select(&rb_sel)
                .next()?
                .text()
                .collect::<String>();
            let rt_text = element_ref
                .select(&rt_sel)
                .next()?
                .text()
                .collect::<String>();
            let rb = normalize_whitespace(&clean_consecutive_slashes(&rb_text));
            let rt = normalize_whitespace(&clean_consecutive_slashes(&rt_text));

            if !rb.is_empty() && !rt.is_empty() {
                Some(HtmlNode::CustomRuby { rb, rt })
            } else {
                None
            }
        }

        "custom-post" => el_val
            .attr("postid")
            .and_then(|s| s.trim().parse().ok())
            .filter(|&id: &EntryId| id > 0)
            .map(|id| HtmlNode::CustomPost { post_id: id }),
        "custom-video" => el_val
            .attr("url")
            .map(|s| s.trim())
            .filter(|url| !url.is_empty())
            .map(|url| HtmlNode::CustomVideo {
                url: url.to_string(),
            }),
        _ => None,
    }
}
