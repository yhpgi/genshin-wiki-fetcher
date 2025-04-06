use crate::constants::*;
use crate::data_structures::{ProcessedListItem, *};
use crate::errors::{AppError, AppResult};
use crate::logging::{log, LogLevel};
use crate::utils::{
    format_cal_date, process_and_filter_detail, process_cal_abstracts_bulk, process_filters_value,
    remove_ids_recursive,
};
use async_recursion::async_recursion;
use bytes::Bytes;
use chrono::Utc;
use futures::future::join_all;
use reqwest::header::HeaderValue;
use reqwest::Client;
use serde::de::DeserializeOwned;
use serde_json::{from_value, json, Value};

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

                    if status.as_u16() == 400 {
                        if resp_text.contains("invalid request param")
                            || resp_text.contains("params error")
                        {
                            return Err(AppError::ApiError {
                                retcode: 100010,
                                message: "Invalid request param".into(),
                            });
                        }
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
            if wrapper.retcode == 100010 && endpoint == "list" {
                return Err(AppError::ApiError {
                    retcode: 100010,
                    message: "Invalid request param".into(),
                });
            }

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
        for res in join_all(tasks).await {
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
            if !matches!(
                e,
                AppError::ApiError {
                    retcode: 100010,
                    ..
                } | AppError::ApiError { .. }
            ) {
                log(
                    LogLevel::Warning,
                    &format!("Bulk fetch partial fail ({}): {:?}", ctx, e),
                );
            }
        }

        Ok(combined)
    }

    pub async fn fetch_entry_detail(
        &self,
        detail_sem: Arc<Semaphore>,
        lang: &str,
        menu_id: MenuId,
        list_entry_id: EntryId,
    ) -> AppResult<Option<OutputDetailPage>> {
        let log_ctx = format!("Entry:{} (Menu:{}) [{}]", list_entry_id, menu_id, lang);
        let params = HashMap::from([("entry_page_id".to_string(), list_entry_id.to_string())]);
        let detail_response_data = {
            let _p = detail_sem.acquire().await.expect("Detail sema fail");
            match self
                .fetch::<DetailResponseData>(
                    reqwest::Method::GET,
                    "detail",
                    lang,
                    Some(&params),
                    None,
                )
                .await
            {
                Ok(data) => data,
                Err(e) => {
                    log(
                        LogLevel::Warning,
                        &format!("Detail Fetch FAIL {}. Err: {:?}. Skip.", log_ctx, e),
                    );
                    return Ok(None);
                }
            }
        };

        let mut raw_detail_page = detail_response_data.page;
        let page_id = list_entry_id;

        if raw_detail_page.id.is_none() {
            raw_detail_page.id = Some(page_id);
        }

        for module in raw_detail_page.modules.iter_mut() {
            for component in &mut module.components {
                remove_ids_recursive(&mut component.data);
            }
        }

        let lang_owned = lang.to_string();
        process_and_filter_detail(raw_detail_page, page_id, menu_id, &lang_owned).await
    }

    pub async fn fetch_calendar(
        &self,
        cal_sem: Arc<Semaphore>,
        lang: &str,
        visions: &HashMap<EntryId, String>,
    ) -> AppResult<OutputCalendarFile> {
        let log_ctx = format!("Calendar [{}]", lang);
        let raw = {
            let _p = cal_sem.acquire().await.expect("Cal sema fail");
            self.fetch::<CalendarResponseData>(reqwest::Method::GET, "calendar", lang, None, None)
                .await?
        };

        let mut temp_cal_list = Vec::new();
        let mut temp_op_list = Vec::new();

        for item in raw.calendar {
            if let Value::Object(map) = item {
                let mut proc = TempCalendarItem::default();
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
                proc.character_abstracts =
                    map.get("character_abstracts").cloned().unwrap_or_default();
                proc.material_abstracts =
                    map.get("material_abstracts").cloned().unwrap_or_default();
                proc.ep_abstracts = map.get("ep_abstracts").cloned().unwrap_or_default();

                if proc.drop_day.is_some()
                    || proc.break_type.is_some()
                    || proc.obtain_method.is_some()
                    || !proc.character_abstracts.is_null()
                    || !proc.material_abstracts.is_null()
                    || !proc.ep_abstracts.is_null()
                {
                    temp_cal_list.push(proc);
                }
            }
        }

        for item in raw.op {
            if let Value::Object(map) = item {
                let mut proc = TempOpItem::default();
                proc.is_birth = map.get("is_birth").and_then(Value::as_bool);
                proc.text = map.get("text").and_then(Value::as_str).map(String::from);
                proc.title = map.get("title").and_then(Value::as_str).map(String::from);
                proc.start_time = format_cal_date(map.get("start_time"));
                proc.end_time = format_cal_date(map.get("end_time"));
                proc.ep_abstracts = map.get("ep_abstracts").cloned().unwrap_or_default();

                if proc.is_birth.is_some()
                    || proc.text.is_some()
                    || proc.title.is_some()
                    || proc.start_time.is_some()
                    || !proc.ep_abstracts.is_null()
                {
                    temp_op_list.push(proc);
                }
            }
        }

        let dummy_bulk_data = HashMap::new();
        let final_cal_list = temp_cal_list
            .into_iter()
            .map(|item| {
                let mut final_item = OutputCalendarItem::default();
                final_item.drop_day = item.drop_day;
                final_item.break_type = item.break_type;
                final_item.obtain_method = item.obtain_method;
                final_item.character_abstracts = process_cal_abstracts_bulk(
                    &item.character_abstracts,
                    visions,
                    &dummy_bulk_data,
                );
                final_item.material_abstracts =
                    process_cal_abstracts_bulk(&item.material_abstracts, visions, &dummy_bulk_data);
                final_item.ep_abstracts =
                    process_cal_abstracts_bulk(&item.ep_abstracts, visions, &dummy_bulk_data);
                final_item
            })
            .filter(|item| {
                item.drop_day.is_some()
                    || item.break_type.is_some()
                    || item.obtain_method.is_some()
                    || !item.character_abstracts.is_empty()
                    || !item.material_abstracts.is_empty()
                    || !item.ep_abstracts.is_empty()
            })
            .collect::<Vec<_>>();

        let final_op_list = temp_op_list
            .into_iter()
            .map(|item| {
                let mut final_item = OutputOpItem::default();
                final_item.is_birth = item.is_birth;
                final_item.text = item.text;
                final_item.title = item.title;
                final_item.start_time = item.start_time;
                final_item.end_time = item.end_time;
                final_item.ep_abstracts =
                    process_cal_abstracts_bulk(&item.ep_abstracts, visions, &dummy_bulk_data);
                final_item
            })
            .filter(|item| {
                item.is_birth.is_some()
                    || item.text.is_some()
                    || item.title.is_some()
                    || item.start_time.is_some()
                    || !item.ep_abstracts.is_empty()
            })
            .collect::<Vec<_>>();

        if final_cal_list.is_empty() && final_op_list.is_empty() {
            log(LogLevel::Warning, &format!("{} Empty.", log_ctx));
        }

        Ok(OutputCalendarFile {
            version: Utc::now(),
            language: lang.to_string(),
            calendar: final_cal_list,
            op: final_op_list,
        })
    }
}

pub async fn fetch_menu_list(
    client: Arc<ApiClient>,
    l_sem: Arc<Semaphore>,
    lang: String,
    id: MenuId,
    name: String,
    dir: PathBuf,
) -> AppResult<Option<OutputListFile>> {
    let mut raw_list_items: Vec<ListItem> = Vec::new();
    let mut total = None;
    let mut page = 1;
    let ctx = format!("Menu:{} ('{}') [{}]", id, name, lang);

    loop {
        let adjusted_menu_id = if id == 0 { 9 } else { id };
        let payload = json!({
            "menu_id": adjusted_menu_id,
            "page_num": page,
            "page_size": PAGE_SIZE,
            "use_es": true,
            "filters": if adjusted_menu_id == 9 { Some(json!([])) } else { None }
        });

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

            match result {
                Ok(data) => data,
                Err(AppError::ApiError {
                    retcode: 100010, ..
                }) => {
                    break;
                }

                Err(e) => return Err(e),
            }
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

        raw_list_items.extend(current_page_list_items);

        let current_total = total.unwrap_or(i64::MAX);
        if (raw_list_items.len() as i64) >= current_total {
            if (raw_list_items.len() as i64) > current_total {
                log(
                    LogLevel::Warning,
                    &format!(
                        "List Count WARN {}. Fetched {}, expected {}. Truncate.",
                        ctx,
                        raw_list_items.len(),
                        current_total
                    ),
                );
                raw_list_items.truncate(current_total.try_into().unwrap_or(raw_list_items.len()));
            }

            break;
        }

        page += 1;
    }

    if raw_list_items.is_empty() {
        return Ok(None);
    }

    let mut final_processed_items: Vec<ProcessedListItem> =
        Vec::with_capacity(raw_list_items.len());
    for item in raw_list_items {
        let processed_filters = process_filters_value(&serde_json::to_value(&item.filter_values)?);
        final_processed_items.push(ProcessedListItem {
            id: item.entry_page_id,
            name: item.name,
            icon_url: item.icon_url,
            desc: item.desc,
            display_field: item.display_field,
            filter_values: processed_filters,
        });
    }

    let out = OutputListFile {
        version: Utc::now(),
        language: lang.clone(),
        menu_id: id,
        menu_name: name.clone(),
        total_items: final_processed_items.len(),
        list: final_processed_items,
    };
    let fpath = dir.join(&lang).join(format!(
        "{}.json",
        crate::utils::clean_filename(&id.to_string())
    ));

    if crate::utils::save_json(&fpath, out.clone(), &format!("List {}", ctx)).await? {
        Ok(Some(out))
    } else {
        Ok(None)
    }
}

#[derive(Default, Debug)]
struct TempCalendarItem {
    drop_day: Option<String>,
    break_type: Option<String>,
    obtain_method: Option<String>,
    character_abstracts: Value,
    material_abstracts: Value,
    ep_abstracts: Value,
}

#[derive(Default, Debug)]
struct TempOpItem {
    is_birth: Option<bool>,
    text: Option<String>,
    title: Option<String>,
    start_time: Option<String>,
    end_time: Option<String>,
    ep_abstracts: Value,
}
