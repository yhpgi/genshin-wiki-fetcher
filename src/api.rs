use crate::constants::*;
use crate::data_structures::{ProcessedListItem, *};
use crate::errors::{AppError, AppResult};
use crate::logging::{log, LogLevel};
use crate::utils::{
    clean_filename, collect_nested_ep_ids, format_cal_date, process_and_filter_detail,
    process_cal_abstracts_value, process_filters_value, update_recursive_with_fallback,
};
use async_recursion::async_recursion;
use bytes::Bytes;
use chrono::Utc;
use reqwest::header::HeaderValue;
use reqwest::Client;
use serde::de::DeserializeOwned;
use serde_json::{from_value, json, to_value, Value};
use simd_json::prelude::*;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Semaphore;
use tokio::time::sleep;

#[derive(Clone)]
pub struct ApiClient {
    client: Client,
}

impl ApiClient {
    pub fn new() -> AppResult<Self> {
        let client = Client::builder()
            .timeout(Duration::from_secs(HTTP_TIMEOUT_SECONDS))
            .connect_timeout(Duration::from_secs(HTTP_CONNECT_TIMEOUT))
            .build()?;
        Ok(ApiClient { client })
    }

    #[async_recursion]
    async fn fetch_api_internal(
        &self,
        method: reqwest::Method,
        url: &str,
        lang: &str,
        params: Option<&HashMap<String, String>>,
        json_payload: Option<&Value>,
        attempt: u32,
    ) -> AppResult<Bytes> {
        let url_tag = url
            .split('?')
            .next()
            .unwrap_or(url)
            .split('/')
            .last()
            .unwrap_or("unknown");
        let log_prefix = format!("API Req [{}] {} {}", lang, method, url_tag);
        let mut headers = BASE_UA_HEADERS.clone();
        headers.insert(
            "x-rpc-language",
            HeaderValue::from_str(lang)
                .map_err(|_| AppError::Unexpected("Invalid lang header".into()))?,
        );
        let mut request_builder = self.client.request(method.clone(), url).headers(headers);
        if let Some(p) = params {
            request_builder = request_builder.query(p);
        }

        if let Some(payload) = json_payload {
            request_builder = request_builder.json(payload);
        }

        match request_builder.send().await {
            Ok(resp) => {
                let status = resp.status();
                if status.is_success() {
                    resp.bytes().await.map_err(AppError::Reqwest)
                } else {
                    let resp_text = resp.text().await.unwrap_or_else(|_| "[text error]".into());
                    let err_name = status.canonical_reason().unwrap_or("Http Error");
                    let level = if attempt < MAX_RETRIES {
                        LogLevel::Warning
                    } else {
                        LogLevel::Error
                    };
                    let mut msg = format!("{} - {} (Status: {})", log_prefix, err_name, status);
                    if status.as_u16() == 413 {
                        return Err(AppError::ApiError {
                            retcode: 413,
                            message: "Payload too large".into(),
                        });
                    }

                    msg.push_str(&format!(
                        " (Try {}/{}) Resp: {}...",
                        attempt + 1,
                        MAX_RETRIES + 1,
                        resp_text.chars().take(100).collect::<String>()
                    ));
                    if attempt >= MAX_RETRIES {
                        log(level, &msg);
                        return Err(AppError::ApiError {
                            retcode: status.as_u16() as i64,
                            message: format!("HTTP Error {} after retries", status),
                        });
                    } else {
                        log(level, &msg);
                        sleep(Duration::from_secs_f32(1.0 * (attempt + 1) as f32)).await;
                        self.fetch_api_internal(
                            method.clone(),
                            url,
                            lang,
                            params,
                            json_payload,
                            attempt + 1,
                        )
                        .await
                    }
                }
            }

            Err(e) => {
                let err_name = if e.is_timeout() {
                    "Timeout"
                } else if e.is_connect() {
                    "Connection"
                } else {
                    "Request"
                };
                let level = if attempt < MAX_RETRIES {
                    LogLevel::Warning
                } else {
                    LogLevel::Error
                };
                let msg = format!(
                    "{} - {} Error (Try {}/{})",
                    log_prefix,
                    err_name,
                    attempt + 1,
                    MAX_RETRIES + 1
                );

                if attempt >= MAX_RETRIES {
                    log(level, &format!("{} Err: {}", msg, e));
                    if e.is_timeout() {
                        Err(AppError::Timeout)
                    } else {
                        Err(AppError::Reqwest(e))
                    }
                } else {
                    log(level, &msg);
                    sleep(Duration::from_secs_f32(1.0 * (attempt + 1) as f32)).await;
                    self.fetch_api_internal(
                        method.clone(),
                        url,
                        lang,
                        params,
                        json_payload,
                        attempt + 1,
                    )
                    .await
                }
            }
        }
    }

    pub async fn fetch<T: DeserializeOwned>(
        &self,
        method: reqwest::Method,
        endpoint: &str,
        lang: &str,
        params: Option<&HashMap<String, String>>,
        payload: Option<&Value>,
    ) -> AppResult<T> {
        let url = API_ENDPOINTS
            .get(endpoint)
            .ok_or_else(|| AppError::Argument(format!("Invalid endpoint key: {}", endpoint)))?;
        let mut bytes_vec = self
            .fetch_api_internal(method.clone(), url, lang, params, payload, 0)
            .await?
            .to_vec();
        let wrapper: ApiResponseWrapper =
            simd_json::from_slice(&mut bytes_vec).map_err(|e| AppError::SimdJsonParse(e))?;
        if wrapper.retcode != 0 {
            return Err(AppError::ApiError {
                retcode: wrapper.retcode,
                message: wrapper.message,
            });
        }

        let data = wrapper.data.ok_or_else(|| {
            AppError::ApiResponseInvalid(format!("Missing 'data' for {} [{}]", endpoint, lang))
        })?;
        serde_json::from_value(data)
            .map_err(|e| AppError::Processing(format!("Data deserialize error: {}", e)))
    }

    pub async fn fetch_nav(&self, lang: &str) -> AppResult<Vec<NavMenuItem>> {
        let data: NavResponseData = self
            .fetch(reqwest::Method::GET, "nav", lang, None, None)
            .await?;
        let mut menus = Vec::with_capacity(data.nav.len());
        let mut ids = HashSet::new();
        for entry in data.nav {
            let entry_name_opt = entry.name.clone();
            if let Some((menu, name)) = entry.menu.as_ref().zip(entry_name_opt) {
                if !name.is_empty() {
                    if ids.insert(menu.menu_id) {
                        menus.push(NavMenuItem {
                            menu_id: menu.menu_id,
                            name,
                            icon_url: entry.icon_url.unwrap_or_default(),
                        });
                    }
                }
            } else {
                log(
                    LogLevel::Warning,
                    &format!("Nav WARN [{}]. Skipping invalid item: {:?}", lang, entry),
                );
            }
        }

        if menus.is_empty() {
            log(LogLevel::Warning, &format!("Nav Empty [{}].", lang));
        }

        menus.sort_unstable_by_key(|m| m.menu_id);
        Ok(menus)
    }

    pub async fn fetch_bulk_data(
        &self,
        sem: Arc<Semaphore>,
        ids: &HashSet<EntryId>,
        lang: &str,
        ctx: &str,
    ) -> AppResult<HashMap<EntryId, Value>> {
        if ids.is_empty() {
            return Ok(HashMap::new());
        }

        let mut sorted_ids: Vec<EntryId> = ids.iter().cloned().collect();
        sorted_ids.sort_unstable();
        let total = (sorted_ids.len() + BULK_BATCH_SIZE - 1) / BULK_BATCH_SIZE;
        let mut tasks = Vec::new();
        for (i, batch) in sorted_ids.chunks(BULK_BATCH_SIZE).enumerate() {
            let params = HashMap::from([(
                "str_entry_page_ids".to_string(),
                batch
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(","),
            )]);
            let client = self.clone();
            let lang = lang.to_string();
            let sema = sem.clone();
            let b_ctx = format!("{} Batch {}/{}", ctx, i + 1, total);
            let _b_ids = batch.to_vec();
            tasks.push(tokio::spawn(async move {
                let _p = sema.acquire().await.expect("Bulk sema fail");
                match client
                    .fetch::<BulkResponseData>(
                        reqwest::Method::GET,
                        "bulk",
                        &lang,
                        Some(&params),
                        None,
                    )
                    .await
                {
                    Ok(d) => Ok(d
                        .entry_pages
                        .into_iter()
                        .filter_map(|e| {
                            let mut map = serde_json::Map::new();
                            e.name.map(|n| map.insert("name".into(), json!(n)));
                            e.desc.map(|d| map.insert("desc".into(), json!(d)));
                            e.icon_url.map(|i| map.insert("icon_url".into(), json!(i)));
                            if !map.is_empty() {
                                Some((e.id, Value::Object(map)))
                            } else {
                                None
                            }
                        })
                        .collect::<HashMap<_, _>>()),
                    Err(e) => {
                        log(
                            LogLevel::Warning,
                            &format!("{} FAIL [{}]. Err: {:?}", b_ctx, lang, e),
                        );
                        Err(e)
                    }
                }
            }));
        }

        let mut combined = HashMap::new();
        let mut first_err = None;
        for res in futures::future::join_all(tasks).await {
            match res {
                Ok(Ok(batch_res)) => combined.extend(batch_res),
                Ok(Err(e)) => {
                    if first_err.is_none() {
                        first_err = Some(e);
                    }
                }

                Err(join_err) => {
                    log(LogLevel::Error, &format!("Bulk task panic: {}", join_err));
                    if first_err.is_none() {
                        first_err = Some(AppError::JoinError(join_err));
                    }
                }
            }
        }

        if let Some(e) = first_err {
            log(
                LogLevel::Warning,
                &format!("Bulk fetch partial fail: {:?}", e),
            );
        }

        Ok(combined)
    }

    pub async fn refresh_fields_from_bulk(
        &self,
        sem: Arc<Semaphore>,
        data: &mut Value,
        lang: &str,
        ctx: &str,
    ) -> AppResult<bool> {
        let mut ids = HashSet::new();
        collect_nested_ep_ids(data, &mut ids, 0)?;
        if ids.is_empty() {
            return Ok(false);
        }

        let mut primary = self
            .fetch_bulk_data(sem.clone(), &ids, lang, &format!("{} Primary", ctx))
            .await?;
        let fallback_ids: HashSet<EntryId> = ids
            .iter()
            .filter(|id| {
                primary
                    .get(id)
                    .and_then(|v| v.get("icon_url"))
                    .and_then(|v| v.as_str())
                    .map_or(true, str::is_empty)
            })
            .cloned()
            .collect();
        if !fallback_ids.is_empty() {
            let mut processed = HashSet::new();
            let all_langs_clone: Vec<String> = SUPPORTED_LANGS.iter().cloned().collect();
            for alt_lang_owned in all_langs_clone {
                if alt_lang_owned == lang {
                    continue;
                }

                let needed: HashSet<EntryId> =
                    fallback_ids.difference(&processed).cloned().collect();
                if needed.is_empty() {
                    break;
                }

                let alt = &alt_lang_owned;
                log(
                    LogLevel::Info,
                    &format!("{} Icon Alt ({}) for {} IDs", ctx, alt, needed.len()),
                );
                let fallback = self
                    .fetch_bulk_data(
                        sem.clone(),
                        &needed,
                        alt,
                        &format!("{} Icon Alt ({})", ctx, alt),
                    )
                    .await?;
                for (id, item) in fallback {
                    if needed.contains(&id) {
                        if let Some(icon) = item.get("icon_url").and_then(|v| v.as_str()) {
                            if !icon.is_empty() {
                                let entry = primary.entry(id).or_insert_with(|| json!({}));
                                if let Value::Object(map) = entry {
                                    if map
                                        .get("icon_url")
                                        .and_then(|v| v.as_str())
                                        .map_or(true, str::is_empty)
                                    {
                                        map.insert("icon_url".into(), json!(icon));
                                    }
                                }

                                processed.insert(id);
                            }
                        }
                    }
                }
            }

            if fallback_ids.len() > processed.len() {
                log(
                    LogLevel::Warning,
                    &format!(
                        "{} Icon Alt [{}] failed for {} IDs.",
                        ctx,
                        lang,
                        fallback_ids.len() - processed.len()
                    ),
                );
            }
        }

        if primary.is_empty() {
            log(
                LogLevel::Warning,
                &format!("{} [{}] No bulk data found.", ctx, lang),
            );
            return Ok(false);
        }

        update_recursive_with_fallback(data, &primary, 0)
    }

    pub async fn fetch_entry_detail(
        &self,
        detail_sem: Arc<Semaphore>,
        bulk_sem: Arc<Semaphore>,
        lang: &str,
        menu_id: MenuId,
        id: EntryId,
    ) -> AppResult<Option<OutputDetailPage>> {
        let log_ctx = format!("Entry:{} (Menu:{}) [{}]", id, menu_id, lang);
        let params = HashMap::from([("entry_page_id".to_string(), id.to_string())]);
        let _p = detail_sem.acquire().await.expect("Detail sema fail");
        let raw = match self
            .fetch::<DetailResponseData>(reqwest::Method::GET, "detail", lang, Some(&params), None)
            .await
        {
            Ok(data) => data.page,
            Err(e) => {
                log(
                    LogLevel::Warning,
                    &format!("Detail Fetch FAIL {}. Err: {:?}. Skip.", log_ctx, e),
                );
                return Ok(None);
            }
        };
        let mut val = serde_json::to_value(&raw).map_err(AppError::SerdeJsonSerialize)?;
        let refreshed_raw_page = match self
            .refresh_fields_from_bulk(
                bulk_sem.clone(),
                &mut val,
                lang,
                &format!("Detail Refresh {}", log_ctx),
            )
            .await
        {
            Ok(true) => serde_json::from_value(val).map_err(|e| {
                AppError::Processing(format!("Detail refresh deserialize err: {}", e))
            })?,
            Ok(false) => raw,
            Err(e) => {
                log(
                    LogLevel::Error,
                    &format!(
                        "Detail Refresh FAIL {}. Err: {:?}. Process original.",
                        log_ctx, e
                    ),
                );
                raw
            }
        };
        let lang_owned = lang.to_string();
        process_and_filter_detail(refreshed_raw_page, &lang_owned).await
    }

    pub async fn fetch_calendar(
        &self,
        cal_sem: Arc<Semaphore>,
        lang: &str,
        visions: &HashMap<EntryId, String>,
    ) -> AppResult<OutputCalendarFile> {
        let log_ctx = format!("Calendar [{}]", lang);
        let _p = cal_sem.acquire().await.expect("Cal sema fail");
        let raw = self
            .fetch::<CalendarResponseData>(reqwest::Method::GET, "calendar", lang, None, None)
            .await?;
        let mut cal_list = Vec::with_capacity(raw.calendar.len());
        let mut op_list = Vec::with_capacity(raw.op.len());
        for item in raw.calendar {
            if let Value::Object(map) = item {
                let mut proc = OutputCalendarItem::default();
                proc.drop_day = map
                    .get("drop_day")
                    .and_then(Value::as_str)
                    .map(String::from);
                proc.break_type = map
                    .get("break_type")
                    .and_then(Value::as_str)
                    .map(String::from);
                proc.obtain_method = map
                    .get("obtain_method")
                    .and_then(Value::as_str)
                    .map(String::from);
                proc.character_abstracts = process_cal_abstracts_value(
                    map.get("character_abstracts"),
                    lang,
                    "cal char",
                    visions,
                );
                proc.material_abstracts = process_cal_abstracts_value(
                    map.get("material_abstracts"),
                    lang,
                    "cal mat",
                    visions,
                );
                proc.ep_abstracts =
                    process_cal_abstracts_value(map.get("ep_abstracts"), lang, "cal ep", visions);
                if proc.drop_day.is_some()
                    || !proc.character_abstracts.is_empty()
                    || !proc.material_abstracts.is_empty()
                    || !proc.ep_abstracts.is_empty()
                {
                    cal_list.push(proc);
                }
            }
        }

        for item in raw.op {
            if let Value::Object(map) = item {
                let mut proc = OutputOpItem::default();
                proc.is_birth = map.get("is_birth").and_then(Value::as_bool);
                proc.text = map.get("text").and_then(Value::as_str).map(String::from);
                proc.title = map.get("title").and_then(Value::as_str).map(String::from);
                proc.start_time = format_cal_date(map.get("start_time"));
                proc.end_time = format_cal_date(map.get("end_time"));
                proc.ep_abstracts =
                    process_cal_abstracts_value(map.get("ep_abstracts"), lang, "op ep", visions);
                if proc.is_birth.is_some()
                    || proc.text.is_some()
                    || proc.title.is_some()
                    || proc.start_time.is_some()
                    || !proc.ep_abstracts.is_empty()
                {
                    op_list.push(proc);
                }
            }
        }

        if cal_list.is_empty() && op_list.is_empty() {
            log(LogLevel::Warning, &format!("{} Empty.", log_ctx));
        }

        Ok(OutputCalendarFile {
            version: Utc::now(),
            language: lang.to_string(),
            calendar: cal_list,
            op: op_list,
        })
    }
}

pub async fn fetch_menu_list(
    client: Arc<ApiClient>,
    l_sem: Arc<Semaphore>,
    b_sem: Arc<Semaphore>,
    lang: String,
    id: MenuId,
    name: String,
    dir: PathBuf,
) -> AppResult<Option<OutputListFile>> {
    let mut final_processed_items: Vec<ProcessedListItem> = Vec::new();
    let mut total = None;
    let mut page = 1;
    let ctx = format!("Menu:{} ('{}') [{}]", id, name, lang);

    loop {
        let payload = json!({"menu_id": if id==0{9}else{id}, "page_num": page, "page_size": PAGE_SIZE, "use_es": true, "filters": if id==9||id==0{Some(json!([]))}else{None}});
        let resp_data = {
            let _permit = l_sem.acquire().await.map_err(|e| {
                AppError::Unexpected(format!("List semaphore acquire failed: {}", e))
            })?;
            let result = client
                .fetch::<ListResponseData>(
                    reqwest::Method::POST,
                    "list",
                    &lang,
                    None,
                    Some(&payload),
                )
                .await;
            if !LIST_FETCH_DELAY.is_zero() {
                sleep(LIST_FETCH_DELAY).await;
            }

            result?
        };

        if total.is_none() {
            total = resp_data.total;
            if total == Some(0) {
                log(LogLevel::Info, &format!("List Empty {}.", ctx));
                return Ok(None);
            }
        }

        let current_page_list_items: Vec<ListItem> = match from_value(resp_data.list) {
            Ok(items_vec) => items_vec,
            Err(e) => {
                log(
                    LogLevel::Error,
                    &format!(
                        "List Page {} Parse FAIL {}. Err: {}. Skipping page.",
                        page, ctx, e
                    ),
                );
                break;
            }
        };

        let count = current_page_list_items.len();
        if count == 0 {
            break;
        }

        let page_processed_items: Vec<ProcessedListItem> = current_page_list_items.into_iter().map(|item| {
             let filter_values_as_value_map: Result<serde_json::Map<String, Value>, _> = item.filter_values
                 .into_iter()
                 .map(|(k, v)| {
                     to_value(v).map(|value| (k, value))
                 })
                 .collect();

            let processed_filters = match filter_values_as_value_map {
                Ok(map) => FilterValues {
                        filters: process_filters_value(&Value::Object(map))
                    },
                Err(e) => {
                    log(LogLevel::Warning, &format!("Failed to convert RawFilterEntry map to Value for item {}: {}. Using default.", item.entry_page_id, e));
                    FilterValues::default()
                }

            };

             ProcessedListItem {
                 entry_page_id: item.entry_page_id,
                 name: item.name,
                 icon_url: item.icon_url,
                 desc: item.desc,
                 display_field: item.display_field,
                 filter_values: processed_filters,
            }

        }).collect();

        final_processed_items.extend(page_processed_items);

        let current_total = total.unwrap_or(i64::MAX);
        if (final_processed_items.len() as i64) >= current_total {
            if (final_processed_items.len() as i64) > current_total {
                log(
                    LogLevel::Warning,
                    &format!(
                        "List Count WARN {}. Fetched {}, expected {}. Truncate.",
                        ctx,
                        final_processed_items.len(),
                        current_total
                    ),
                );
                final_processed_items.truncate(
                    current_total
                        .try_into()
                        .unwrap_or(final_processed_items.len()),
                );
            }

            break;
        }

        page += 1;
    }

    if final_processed_items.is_empty() {
        return Ok(None);
    }

    let mut val =
        serde_json::to_value(&final_processed_items).map_err(AppError::SerdeJsonSerialize)?;
    match client
        .refresh_fields_from_bulk(b_sem, &mut val, &lang, &format!("List Refresh {}", ctx))
        .await
    {
        Ok(true) => {
            final_processed_items = from_value(val).map_err(|e| {
                AppError::Processing(format!("List refresh deserialize err: {}", e))
            })?;
        }

        Ok(false) => {}

        Err(e) => {
            log(
                LogLevel::Error,
                &format!("List Refresh FAIL {}. Err: {:?}", ctx, e),
            );
        }
    }

    let out = OutputListFile {
        version: Utc::now(),
        language: lang.clone(),
        menu_id: id,
        menu_name: name.clone(),
        total_items: final_processed_items.len(),
        list: final_processed_items,
    };
    let fpath = dir
        .join(&lang)
        .join(format!("{}.json", clean_filename(&id.to_string())));
    if crate::utils::save_json(&fpath, out.clone(), &format!("List {}", ctx)).await? {
        Ok(Some(out))
    } else {
        Ok(None)
    }
}
