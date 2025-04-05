use crate::constants::*;
use crate::data_structures::*;
use crate::errors::{AppError, AppResult};
use crate::logging::{log, LogLevel};
use scraper::Element;
use scraper::{ElementRef, Html, Node, Selector};
use serde::Serialize;
use serde_json::{json, map::Map, Value};
use std::collections::{BTreeSet, HashMap, HashSet};
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
    let result = run_blocking(move || {
        serde_json::to_string_pretty(&data).map_err(AppError::SerdeJsonSerialize)
    })
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
                let final_v = normalize_value_types(v, depth + 1)?;
                let final_v_for_insert = if INT_KEYS.contains(k.as_str()) {
                    match &final_v {
                        Value::Number(n) if n.is_f64() && n.as_f64().unwrap().fract() == 0.0 => {
                            Value::Number(serde_json::Number::from(n.as_i64().unwrap()))
                        }

                        Value::String(s) if NUM_STR_RE.is_match(s.trim()) => s
                            .trim()
                            .parse::<f64>()
                            .ok()
                            .map_or(final_v.clone(), |f_val| {
                                Value::Number(serde_json::Number::from(f_val as i64))
                            }),
                        _ => final_v,
                    }
                } else if k == "data"
                    && !(final_v.is_object() || final_v.is_array())
                    && !final_v.is_null()
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

pub fn transform_comp_data(data: Value, depth: u32) -> AppResult<Value> {
    if depth > MAX_RECURSION_DEPTH {
        return Err(AppError::RecursionLimit {
            context: "transform_comp_data".to_string(),
        });
    }

    match data {
        Value::Object(map) => {
            if map.contains_key("key") && map.get("value").map_or(false, |v| v.is_array()) {
                if let Some(Value::Array(data_values)) = map.get("value") {
                    let mut new_dict = Map::new();
                    if let Some(key_val) = map.get("key") {
                        new_dict.insert(
                            "key".to_string(),
                            transform_comp_data(key_val.clone(), depth + 1)?,
                        );
                    }

                    for (k, v) in map.iter() {
                        if k != "key" && k != "value" && !(k == "id" && v.is_string()) {
                            new_dict.insert(k.clone(), transform_comp_data(v.clone(), depth + 1)?);
                        }
                    }

                    let mut proc_vals = Vec::with_capacity(data_values.len());
                    for item in data_values {
                        proc_vals.push(transform_comp_data(item.clone(), depth + 1)?);
                    }

                    if proc_vals
                        .iter()
                        .all(|v| v.is_string() || v.is_number() || v.is_boolean() || v.is_null())
                    {
                        let string_values: Vec<Value> = proc_vals
                            .into_iter()
                            .map(|v| {
                                Value::String(
                                    v.as_str()
                                        .map(String::from)
                                        .unwrap_or_else(|| v.to_string()),
                                )
                            })
                            .collect();
                        new_dict.insert("string_values".to_string(), Value::Array(string_values));
                        new_dict.insert("item_values".to_string(), Value::Array(vec![]));
                    } else {
                        new_dict.insert("string_values".to_string(), Value::Array(vec![]));
                        new_dict.insert("item_values".to_string(), Value::Array(proc_vals));
                    }

                    return Ok(Value::Object(new_dict));
                }
            }

            let mut new_map = Map::with_capacity(map.len());
            for (k, v) in map.into_iter() {
                if !(k == "id" && v.is_string()) {
                    new_map.insert(k, transform_comp_data(v, depth + 1)?);
                } else {
                    new_map.insert(k, v);
                }
            }

            Ok(Value::Object(new_map))
        }

        Value::Array(vec) => {
            let mut new_vec = Vec::with_capacity(vec.len());
            for item in vec {
                new_vec.push(transform_comp_data(item, depth + 1)?);
            }

            Ok(Value::Array(new_vec))
        }

        _ => Ok(data),
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
            for key in ["ep_id", "entry_page_id"] {
                if let Some(id_val) = map.get(key) {
                    if let Some(id) = id_val.as_i64() {
                        collected_ids.insert(id);
                    } else if let Some(id_str) = id_val.as_str() {
                        if let Ok(id) = id_str.parse() {
                            collected_ids.insert(id);
                        }
                    }
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

pub fn update_recursive_with_fallback(
    data: &mut Value,
    bulk_map: &HashMap<EntryId, Value>,
    depth: u32,
) -> AppResult<bool> {
    if depth > MAX_RECURSION_DEPTH {
        return Err(AppError::RecursionLimit {
            context: "update_recursive_with_fallback".to_string(),
        });
    }

    let mut modified = false;
    if let Value::Object(map) = data {
        let current_id: Option<EntryId> = ["ep_id", "entry_page_id", "id"].iter().find_map(|key| {
            map.get(*key).and_then(|v| {
                v.as_i64()
                    .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
            })
        });
        if let Some(id) = current_id {
            if let Some(Value::Object(fetched_map)) = bulk_map.get(&id) {
                if let Some(val) = fetched_map.get("name") {
                    let current_val = map.get("name");
                    if current_val != Some(val) {
                        map.insert("name".to_string(), val.clone());
                        modified = true;
                    }
                }

                if let Some(val) = fetched_map.get("desc") {
                    let current_val = map.get("desc");
                    if current_val != Some(val) {
                        map.insert("desc".to_string(), val.clone());
                        modified = true;
                    }
                }

                if let Some(val) = fetched_map.get("icon_url") {
                    if val.is_string() && !val.as_str().unwrap_or("").is_empty() {
                        for key in ["icon", "icon_url"] {
                            if let Some(current) = map.get_mut(key) {
                                if current != val {
                                    *current = val.clone();
                                    modified = true;
                                    break;
                                } else {
                                    break;
                                }
                            }
                        }
                    }
                }
            }
        }

        let keys: Vec<String> = map.keys().cloned().collect();
        for key in keys {
            if let Some(value) = map.get_mut(&key) {
                if update_recursive_with_fallback(value, bulk_map, depth + 1)? {
                    modified = true;
                }
            }
        }
    } else if let Value::Array(vec) = data {
        for item in vec.iter_mut() {
            if update_recursive_with_fallback(item, bulk_map, depth + 1)? {
                modified = true;
            }
        }
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

fn get_text_styles(element_ref: &ElementRef) -> Vec<String> {
    let mut styles = BTreeSet::new();
    let mut current = Some(*element_ref);
    let mut loop_guard = 0;
    while let Some(parent_ref) = current {
        if loop_guard > 50 {
            log(
                LogLevel::Warning,
                "HTML style ancestor loop guard triggered.",
            );
            break;
        }

        let tag = parent_ref.value().name().to_lowercase();
        match tag.as_str() {
            "strong" | "b" => {
                styles.insert("bold".to_string());
            }

            "em" | "i" => {
                styles.insert("italic".to_string());
            }

            "span" => {
                if let Some(s) = parent_ref.value().attr("style") {
                    if let Some(cap) = STYLE_COLOR_RE.captures(s) {
                        if let Some(c) = cap.get(1) {
                            styles.insert(format!("color:{}", c.as_str().to_lowercase()));
                        }
                    }
                }
            }

            "p" | "div" | "li" | "td" | "th" | "body" | "html" => break,
            t if HEADING_TAGS.contains(t) => break,
            _ => {}
        }

        current = parent_ref.parent_element();
        loop_guard += 1;
    }

    styles.into_iter().collect()
}

pub async fn process_html_in_component_data(
    component: &mut Component,
    _lang: &str,
    _page_id: EntryId,
) -> AppResult<()> {
    if HTML_CONTENT_COMPONENT_IDS.contains(component.component_id.as_str()) {
        let mut html_opt: Option<String> = None;
        let mut requires_conversion = false;
        match &component.data {
            Value::Object(map) => {
                if let Some(Value::Object(dmap)) = map.get("data") {
                    html_opt = dmap
                        .get("content")
                        .and_then(Value::as_str)
                        .map(String::from);
                } else {
                    html_opt = map.get("content").and_then(Value::as_str).map(String::from);
                }
            }

            Value::String(s) => {
                html_opt = Some(s.clone());
                requires_conversion = true;
            }

            _ => {}
        }

        if let Some(html_content) = html_opt {
            if html_content.trim().is_empty() {
                if requires_conversion {
                    component.data = json!({ "parsed_content": [] });
                } else if let Value::Object(map) = &mut component.data {
                    if let Some(Value::Object(dmap)) = map.get_mut("data") {
                        dmap.insert("parsed_content".to_string(), json!([]));
                        dmap.remove("content");
                    } else {
                        map.insert("parsed_content".to_string(), json!([]));
                        map.remove("content");
                    }
                }

                return Ok(());
            }

            let parsed_nodes_result =
                tokio::task::spawn_blocking(move || parse_html_content(&html_content)).await;
            match parsed_nodes_result {
                Ok(Ok(parsed_nodes)) => {
                    let parsed_value =
                        serde_json::to_value(parsed_nodes).map_err(AppError::SerdeJsonSerialize)?;
                    if requires_conversion {
                        component.data = json!({ "parsed_content": parsed_value });
                    } else if let Value::Object(map) = &mut component.data {
                        if let Some(Value::Object(dmap)) = map.get_mut("data") {
                            dmap.insert("parsed_content".to_string(), parsed_value);
                            dmap.remove("content");
                        } else {
                            map.insert("parsed_content".to_string(), parsed_value);
                            map.remove("content");
                        }
                    } else {
                        return Err(AppError::Processing(
                            "HTML conversion logic error after parsing".into(),
                        ));
                    }
                }

                Ok(Err(app_err)) => return Err(app_err),
                Err(join_err) => return Err(AppError::JoinError(join_err)),
            }
        } else if requires_conversion {
            component.data = json!({ "parsed_content": [] });
        }
    }

    Ok(())
}

fn filter_component_inplace(comp: &mut Component) -> bool {
    !comp.component_id.is_empty() && !comp.data.is_null()
}

fn filter_module_inplace(module: &mut Module) -> bool {
    module.components.retain_mut(filter_component_inplace);
    !module.id.is_empty() && (module.name.is_some() || !module.components.is_empty())
}

pub async fn process_and_filter_detail(
    mut raw_page: RawDetailPage,
    lang: &str,
) -> AppResult<Option<OutputDetailPage>> {
    let page_id = raw_page.id;
    let filter_values_map = process_filters_value(&raw_page.filter_values);

    for module in &mut raw_page.modules {
        for component in &mut module.components {
            component.data = normalize_value_types(component.data.take(), 0)?;

            if !HTML_CONTENT_COMPONENT_IDS.contains(component.component_id.as_str()) {
                let data_to_transform = component.data.take();
                component.data =
                    run_blocking(move || transform_comp_data(data_to_transform, 0)).await?;
            } else {
                process_html_in_component_data(component, lang, page_id).await?;
            }
        }
    }

    raw_page.modules.retain_mut(filter_module_inplace);

    let has_content =
        !raw_page.name.is_empty() || raw_page.desc.is_some() || !raw_page.modules.is_empty();

    if !has_content {
        return Ok(None);
    }

    Ok(Some(OutputDetailPage {
        id: raw_page.id,
        name: Some(raw_page.name).filter(|s| !s.is_empty()),
        desc: raw_page.desc,
        icon_url: raw_page.icon_url,
        header_img_url: raw_page.header_img_url,
        modules: raw_page.modules,
        filter_values: filter_values_map,
        menu_id: raw_page.menu_id,
        menu_name: raw_page.menu_name,
        version: raw_page.version,
    }))
}

pub fn process_filters_value(raw_filters_val: &Value) -> HashMap<String, Vec<String>> {
    let mut processed = HashMap::new();
    if let Value::Object(raw_filters) = raw_filters_val {
        for key in LIST_FILTER_FIELDS.iter() {
            if let Some(field_data) = raw_filters.get(*key) {
                let mut values: HashSet<String> = HashSet::new();

                if let Some(Value::Array(vals)) = field_data.get("values") {
                    vals.iter().for_each(|v| {
                        if let Some(s) = v.as_str() {
                            if !s.is_empty() {
                                values.insert(s.to_string());
                            }
                        } else if let Some(n) = v.as_i64() {
                            values.insert(n.to_string());
                        } else if let Some(f) = v.as_f64() {
                            values.insert(f.to_string());
                        } else if let Some(b) = v.as_bool() {
                            values.insert(b.to_string());
                        }
                    });
                } else if let Some(Value::Array(vtypes)) = field_data.get("value_types") {
                    if let Some(Value::Object(ftype)) = vtypes.get(0) {
                        if let Some(fval) = ftype.get("value") {
                            if let Some(s) = fval.as_str() {
                                if !s.is_empty() {
                                    values.insert(s.to_string());
                                }
                            } else if let Some(n) = fval.as_i64() {
                                values.insert(n.to_string());
                            } else if let Some(f) = fval.as_f64() {
                                values.insert(f.to_string());
                            } else if let Some(b) = fval.as_bool() {
                                values.insert(b.to_string());
                            }
                        }
                    }
                }

                if !values.is_empty() {
                    let mut sorted: Vec<String> = values.into_iter().collect();
                    sorted.sort_unstable();
                    processed.insert(key.to_string(), sorted);
                }
            }
        }
    }

    processed
}

pub fn process_cal_abstracts_value(
    abstract_list: Option<&Value>,
    lang: &str,
    ctx: &str,
    vision_map: &HashMap<EntryId, String>,
) -> Vec<CalendarAbstract> {
    let mut simplified = Vec::new();
    if let Some(Value::Array(list)) = abstract_list {
        for val in list {
            if let Value::Object(map) = val {
                let id_res = map.get("entry_page_id").and_then(|v| {
                    v.as_i64()
                        .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
                });
                let name = map.get("name").and_then(Value::as_str).unwrap_or("");
                let icon = map.get("icon_url").and_then(Value::as_str).unwrap_or("");
                if let Some(id) = id_res {
                    if !name.is_empty() && !icon.is_empty() {
                        simplified.push(CalendarAbstract {
                            entry_page_id: id,
                            name: name.into(),
                            icon_url: icon.into(),
                            character_vision: vision_map.get(&id).cloned(),
                        });
                    }
                } else {
                    log(
                        LogLevel::Warning,
                        &format!(
                            "Cal Abstract WARN [{}] Ctx:'{}'. Invalid ID. Abstract: {}",
                            lang,
                            ctx,
                            val.to_string().chars().take(100).collect::<String>()
                        ),
                    );
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

fn parse_html_content(html_string: &str) -> AppResult<Vec<HtmlNode>> {
    let trimmed = html_string.trim();
    if trimmed.is_empty() {
        return Ok(vec![]);
    }

    let wrapped = if !trimmed.to_lowercase().starts_with("<html")
        && !trimmed.to_lowercase().starts_with("<body")
    {
        format!("<body>{}</body>", trimmed)
    } else {
        trimmed.to_string()
    };
    let fragment = Html::parse_fragment(&wrapped);
    let root_element = fragment.root_element();
    parse_element_children(root_element, 0)
}

fn parse_element_children(element_ref: ElementRef<'_>, depth: u32) -> AppResult<Vec<HtmlNode>> {
    if depth > MAX_RECURSION_DEPTH / 2 {
        log(
            LogLevel::Warning,
            &format!("HTML parse depth limit ({}) hit.", MAX_RECURSION_DEPTH / 2),
        );
        return Ok(vec![HtmlNode::Text {
            text: "[DEPTH]".to_string(),
            styles: vec!["color:#ff0000".to_string()],
        }]);
    }

    let mut results = Vec::new();
    for child_node_edge in element_ref.children() {
        match child_node_edge.value() {
            Node::Text(text_node) => {
                let text = text_node.text.trim();
                if !text.is_empty() {
                    let styles = get_text_styles(&element_ref);
                    results.push(HtmlNode::Text {
                        text: text.to_string(),
                        styles,
                    });
                }
            }

            Node::Element(_) => {
                if let Some(child_element_ref) = ElementRef::wrap(child_node_edge) {
                    results.extend(process_specific_element(child_element_ref, depth + 1)?);
                } else {
                    log(
                        LogLevel::Warning,
                        &format!("Failed to wrap child element node"),
                    );
                }
            }

            _ => {}
        }
    }

    results.retain(|item: &HtmlNode| !item.is_empty_text());
    Ok(results)
}

fn process_specific_element(element_ref: ElementRef<'_>, depth: u32) -> AppResult<Vec<HtmlNode>> {
    if depth > MAX_RECURSION_DEPTH / 2 {
        return Ok(vec![HtmlNode::Text {
            text: "[DEPTH]".into(),
            styles: vec!["color:red".into()],
        }]);
    }

    let tag_name = element_ref.value().name().to_lowercase();
    let alignment = get_node_alignment(&element_ref);
    let mut results = Vec::new();
    match tag_name.as_str() {
        "p" => {
            let content = parse_element_children(element_ref, depth)?;
            if !content.is_empty() {
                results.push(HtmlNode::Paragraph { content, alignment });
            }
        }

        h if HEADING_TAGS.contains(h) => {
            let content = parse_element_children(element_ref, depth)?;
            if !content.is_empty() {
                if let Ok(lvl) = h[1..].parse() {
                    results.push(HtmlNode::Heading {
                        level: lvl,
                        content,
                        alignment,
                    });
                }
            }
        }

        "ul" | "ol" => {
            let sel = Selector::parse("li").map_err(|e| AppError::HtmlParsing(e.to_string()))?;
            let mut items = Vec::new();
            for li_el in element_ref.select(&sel) {
                let li_content = parse_element_children(li_el, depth + 1)?;
                if !li_content.is_empty() {
                    items.push(li_content);
                }
            }

            if !items.is_empty() {
                results.push(HtmlNode::List {
                    ordered: tag_name == "ol",
                    items,
                });
            }
        }

        "custom-entry" => {
            let el_val = element_ref.value();
            if let Some(id) = el_val.attr("epid").and_then(|s| s.parse().ok()) {
                if id > 0 {
                    results.push(HtmlNode::CustomEntry {
                        ep_id: id,
                        name: el_val.attr("name").unwrap_or("").into(),
                        icon_url: el_val.attr("icon").unwrap_or("").into(),
                        amount: el_val
                            .attr("amount")
                            .and_then(|s| s.parse().ok())
                            .unwrap_or(0),
                        display_style: el_val.attr("displaystyle").unwrap_or("link").into(),
                        menu_id: el_val.attr("menuid").and_then(|s| s.parse().ok()),
                    });
                }
            } else {
                log(
                    LogLevel::Warning,
                    &format!("Bad custom-entry: {}", element_ref.html()),
                );
            }
        }

        "custom-image" => {
            let el_val = element_ref.value();
            if let Some(url) = el_val.attr("url") {
                results.push(HtmlNode::CustomImage {
                    url: url.into(),
                    alignment: el_val.attr("align").map(String::from),
                });
            }
        }

        "div" | "span" | "strong" | "b" | "em" | "i" | "u" | "a" | "table" | "tbody" | "tr"
        | "td" | "th" | "body" | "html" => {
            results.extend(parse_element_children(element_ref, depth)?);
        }

        "br" => {}

        _ => {
            let content = parse_element_children(element_ref, depth)?;
            if !content.is_empty() {
                results.extend(content);
            }
        }
    }

    Ok(results)
}
