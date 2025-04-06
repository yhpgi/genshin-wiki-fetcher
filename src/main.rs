mod api;
mod constants;
mod data_structures;
mod errors;
mod logging;
mod utils;

use crate::api::{fetch_menu_list, ApiClient};
use crate::constants::*;
use crate::data_structures::*;
use crate::errors::{AppError, AppResult};
use crate::logging::{log, setup_logging, LogLevel};
use crate::utils::{collect_nested_ep_ids, run_blocking, save_json, update_value_with_bulk};

use chrono::Utc;
use clap::Parser;
use fs_extra::dir::remove;
use futures::future::join_all;
use futures::stream::{self, StreamExt};
use serde_json::{to_value, Value};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;
use tokio::fs;
use tokio::sync::Semaphore;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None, after_help=format!("Supported languages: {}", SUPPORTED_LANGS.join(", ")), arg_required_else_help = true)]
struct CliArgs {
    #[arg(short, long, num_args = 1.., required = true, value_delimiter = ' ', name = "LANG")]
    languages: Vec<String>,
}

#[derive(Debug, Clone, Default)]
struct RunCategoryStats {
    ok: usize,
    fail: usize,
    empty: usize,
    total: usize,
    updated_in_bulk: usize,
}

type RunStats = BTreeMap<String, RunCategoryStats>;

#[derive(Default)]
struct InMemoryData {
    navigation: HashMap<String, Vec<NavMenuItem>>,
    lists: HashMap<String, Vec<OutputListFile>>,
    details: HashMap<String, Vec<OutputDetailPage>>,
    calendars: HashMap<String, OutputCalendarFile>,
    all_ids: HashMap<String, HashSet<EntryId>>,
}

type BulkDataMap = HashMap<EntryId, Value>;
type FallbackBulkDataMap = HashMap<String, BulkDataMap>;
type AllBulkData = HashMap<String, (BulkDataMap, FallbackBulkDataMap)>;

async fn setup_dirs(dirs: &HashMap<&str, PathBuf>, langs: &[String]) -> AppResult<()> {
    log(LogLevel::Step, "Preparing output directories...");
    let mut cleaned_count = 0;
    for dpath in dirs.values() {
        fs::create_dir_all(dpath).await?;
    }

    let mut files_to_clean = Vec::new();
    let mut dirs_to_clean = Vec::new();
    for lang in langs {
        files_to_clean.push(dirs["nav"].join(format!("{}.json", lang)));
        files_to_clean.push(dirs["calendar"].join(format!("{}.json", lang)));
        dirs_to_clean.push(dirs["list"].join(lang));
        dirs_to_clean.push(dirs["detail"].join(lang));
    }

    for f in files_to_clean {
        if fs::try_exists(&f).await? {
            if fs::remove_file(&f).await.is_ok() {
                cleaned_count += 1;
            }
        }
    }

    for d in dirs_to_clean {
        let d_for_is_dir = d.clone();
        if run_blocking(move || Ok(d_for_is_dir.is_dir())).await? {
            let d_for_remove = d.clone();
            match run_blocking(move || remove(&d_for_remove).map_err(AppError::FsExtra)).await {
                Ok(_) => cleaned_count += 1,
                Err(e) => {
                    log(
                        LogLevel::Warning,
                        &format!("Failed to remove dir '{}': {:?}", d.display(), e),
                    );
                }
            }
        }
    }

    for lang in langs {
        fs::create_dir_all(dirs["list"].join(lang)).await?;
        fs::create_dir_all(dirs["detail"].join(lang)).await?;
    }

    if cleaned_count > 0 {
        log(
            LogLevel::Info,
            &format!("Cleaned {} previous items.", cleaned_count),
        );
    }

    log(LogLevel::Success, "Directory setup complete.");
    Ok(())
}

fn parse_cli_args() -> AppResult<Vec<String>> {
    let cli = CliArgs::parse();
    let inputs: HashSet<String> = cli
        .languages
        .into_iter()
        .map(|s| s.to_lowercase().trim().to_string())
        .collect();
    if inputs.contains("all") {
        log(LogLevel::Info, "Processing all supported languages.");
        Ok(SUPPORTED_LANGS.clone())
    } else {
        let supported: HashSet<String> = SUPPORTED_LANGS.iter().cloned().collect();
        let (valid_langs, invalid_langs): (Vec<String>, Vec<String>) =
            inputs.into_iter().partition(|l| supported.contains(l));
        if !invalid_langs.is_empty() {
            log(
                LogLevel::Warning,
                &format!("Ignoring invalid languages: {}", invalid_langs.join(", ")),
            );
        }

        if valid_langs.is_empty() {
            return Err(AppError::Argument("No valid languages specified.".into()));
        }

        let mut sorted_langs = valid_langs;
        sorted_langs.sort_unstable();
        log(
            LogLevel::Info,
            &format!("Selected languages: {}", sorted_langs.join(", ")),
        );
        Ok(sorted_langs)
    }
}

#[tokio::main]
async fn main() -> Result<(), ()> {
    let exit_code = match main_async().await {
        Ok(code) => code,
        Err(e) => {
            log(LogLevel::Error, &format!("FATAL ERROR: {:?}", e));
            1
        }
    };
    log(LogLevel::Info, &format!("Exiting with code {}.", exit_code));
    if exit_code == 0 {
        Ok(())
    } else {
        Err(())
    }
}

async fn main_async() -> AppResult<i32> {
    setup_logging();
    let target_langs = parse_cli_args()?;
    let start_time = Instant::now();
    let start_ts_str = Utc::now().format("%Y-%m-%d %H:%M:%S %Z").to_string();
    log(
        LogLevel::Step,
        &format!(
            "Starting fetch ({} langs) at {}",
            target_langs.len(),
            start_ts_str
        ),
    );

    let dirs: HashMap<&str, PathBuf> = HashMap::from([
        ("nav", PathBuf::from(OUT_DIR).join("navigation")),
        ("list", PathBuf::from(OUT_DIR).join("list")),
        ("detail", PathBuf::from(OUT_DIR).join("detail")),
        ("calendar", PathBuf::from(OUT_DIR).join("calendar")),
    ]);
    setup_dirs(&dirs, &target_langs).await?;

    let client = Arc::new(ApiClient::new()?);
    let list_sem = Arc::new(Semaphore::new(MAX_LIST_CONCUR));
    let detail_sem = Arc::new(Semaphore::new(MAX_DETAIL_CONCUR));
    let bulk_sem = Arc::new(Semaphore::new(MAX_BULK_CONCUR));
    let cal_sem = Arc::new(Semaphore::new(5));

    let mut stats = RunStats::new();
    stats.insert("Navigation".into(), Default::default());
    stats.insert("List".into(), Default::default());
    stats.insert("Detail".into(), Default::default());
    stats.insert("Calendar".into(), Default::default());
    stats.insert("Bulk Fetch".into(), Default::default());
    stats.insert("Save".into(), Default::default());

    let mut data_store = InMemoryData::default();

    log(
        LogLevel::Step,
        &format!("--- Phase 1: Navigation ({} langs) ---", target_langs.len()),
    );
    let mut nav_tasks = Vec::new();
    stats.get_mut("Navigation").unwrap().total = target_langs.len();
    for lang in target_langs.iter() {
        let c = client.clone();
        let l_clone = lang.clone();
        let d = dirs["nav"].clone();
        nav_tasks.push(tokio::spawn(async move {
            let result = c.fetch_nav(&l_clone).await;
            (l_clone, d, result)
        }));
    }

    let mut nav_save_tasks = Vec::new();
    for handle in nav_tasks {
        match handle.await {
            Ok((lang, dir, Ok(menus_data))) => {
                if menus_data.is_empty() {
                    log(LogLevel::Warning, &format!("Nav OK but EMPTY [{}]", lang));
                    stats.get_mut("Navigation").unwrap().fail += 1;
                } else {
                    let path = dir.join(format!("{}.json", lang));
                    let menus_to_save = menus_data.clone();
                    let lang_clone = lang.clone();
                    nav_save_tasks.push(tokio::spawn(async move {
                        (
                            lang_clone.clone(),
                            save_json(&path, menus_to_save, &format!("Nav [{}]", lang_clone)).await,
                        )
                    }));
                    data_store.navigation.insert(lang, menus_data);
                    stats.get_mut("Navigation").unwrap().ok += 1;
                }
            }

            Ok((lang, _, Err(e))) => {
                log(
                    LogLevel::Warning,
                    &format!("Nav FAIL [{}]. Err: {:?}", lang, e),
                );
                stats.get_mut("Navigation").unwrap().fail += 1;
            }

            Err(e) => {
                log(LogLevel::Error, &format!("Nav Task Panic! {}", e));
                stats.get_mut("Navigation").unwrap().fail += 1;
            }
        }
    }

    stats.get_mut("Save").unwrap().total += nav_save_tasks.len();
    for handle in nav_save_tasks {
        match handle.await {
            Ok((_, Ok(true))) => {
                stats.get_mut("Save").unwrap().ok += 1;
            }

            Ok((lang, Ok(false))) => {
                log(
                    LogLevel::Error,
                    &format!("Nav Save FAIL (silent) [{}]", lang),
                );
                stats.get_mut("Save").unwrap().fail += 1;
            }

            Ok((lang, Err(e))) => {
                log(
                    LogLevel::Error,
                    &format!("Nav Save FAIL (error) [{}]. Err: {:?}", lang, e),
                );
                stats.get_mut("Save").unwrap().fail += 1;
            }

            Err(e) => {
                log(LogLevel::Error, &format!("Nav Save Task Panic! {}", e));
                stats.get_mut("Save").unwrap().fail += 1;
            }
        }
    }

    let nav_s = stats["Navigation"].clone();
    log(
        if nav_s.fail == 0 && nav_s.ok > 0 {
            LogLevel::Success
        } else {
            LogLevel::Info
        },
        &format!(
            "Nav phase complete. {} OK, {} Failed/Empty.",
            nav_s.ok, nav_s.fail
        ),
    );

    log(LogLevel::Step, "--- Phase 2: Fetch Menu Lists ---");
    let mut list_tasks = Vec::new();
    let mut total_list_tasks = 0;
    for (lang, menus) in data_store.navigation.iter() {
        for menu in menus {
            total_list_tasks += 1;
            let c = client.clone();
            let ls = list_sem.clone();
            let l = lang.clone();
            let mid = menu.menu_id;
            let mn = menu.name.clone();
            list_tasks.push(tokio::spawn(async move {
                fetch_menu_list(c, ls, l, mid, mn).await
            }));
        }
    }

    stats.get_mut("List").unwrap().total = total_list_tasks;
    if total_list_tasks > 0 {
        log(
            LogLevel::Info,
            &format!("Submitting {} List tasks...", total_list_tasks),
        );
        let mut processed = 0;
        let list_stream = stream::iter(list_tasks).buffer_unordered(MAX_LIST_CONCUR * 2);
        list_stream
            .for_each(|res| {
                processed += 1;
                match res {
                    Ok(Ok(Some(data))) => {
                        let lang = data.language.clone();
                        let lang_ids = data_store.all_ids.entry(lang.clone()).or_default();
                        for item in &data.list {
                            lang_ids.insert(item.id);
                            if let Some(df) = &item.display_field {
                                if let Err(e) = collect_nested_ep_ids(df, lang_ids, 0) {
                                    log(LogLevel::Warning, &format!("Failed collecting IDs from list display_field [{}/{}]: {:?}", lang, item.id, e));
                                }
                            }
                             if let Err(e) = collect_nested_ep_ids(&item.filter_values, lang_ids, 0) {
                                 log(LogLevel::Warning, &format!("Failed collecting IDs from list filter_values [{}/{}]: {:?}", lang, item.id, e));
                             }
                        }
                        data_store
                            .lists
                            .entry(lang)
                            .or_default()
                            .push(data);
                        stats.get_mut("List").unwrap().ok += 1;
                    }
                    Ok(Ok(None)) => {
                        stats.get_mut("List").unwrap().empty += 1;
                    }
                    Ok(Err(AppError::ApiError {
                        retcode: 100010, ..
                    })) => {
                        stats.get_mut("List").unwrap().empty += 1;
                    }
                    Ok(Err(e)) => {
                        log(LogLevel::Error, &format!("List Task Err: {:?}", e));
                        stats.get_mut("List").unwrap().fail += 1;
                    }
                    Err(e) => {
                        log(LogLevel::Error, &format!("List Task Panic! {}", e));
                        stats.get_mut("List").unwrap().fail += 1;
                    }
                }

                if processed % 50 == 0 || processed == total_list_tasks {
                    let s = stats["List"].clone();
                    log(
                        LogLevel::Info,
                        &format!(
                            "List progress: {}/{}. ({} OK, {} Empty/Skip, {} Failed)",
                            processed, s.total, s.ok, s.empty, s.fail
                        ),
                    );
                }
                futures::future::ready(())
            })
            .await;

        let list_s = stats["List"].clone();
        log(
            if list_s.fail == 0 && list_s.ok > 0 {
                LogLevel::Success
            } else {
                LogLevel::Info
            },
            &format!(
                "List phase complete. {} OK, {} Empty/Skip, {} Failed.",
                list_s.ok, list_s.empty, list_s.fail
            ),
        );
    } else {
        log(LogLevel::Info, "No lists to fetch.");
    }

    log(
        LogLevel::Step,
        "Processing list results for vision mapping...",
    );
    let mut vision_maps: HashMap<String, HashMap<EntryId, String>> = HashMap::new();
    for (lang, list_files) in &data_store.lists {
        let map = vision_maps.entry(lang.clone()).or_default();
        for list_data in list_files {
            for item in &list_data.list {
                if let Some(fv_map) = item.filter_values.as_object() {
                    if let Some(Value::String(v)) = fv_map.get("character_vision") {
                        if !v.is_empty() {
                            map.insert(item.id, v.clone());
                        }
                    }
                }
            }
        }
    }

    log(LogLevel::Step, "--- Phase 3: Fetch Entry Details ---");
    let mut detail_tasks = Vec::new();
    let mut total_detail_tasks = 0;
    for (lang, list_files) in data_store.lists.iter() {
        for list_data in list_files {
            for item in &list_data.list {
                total_detail_tasks += 1;
                let c = client.clone();
                let ds = detail_sem.clone();
                let l = lang.clone();
                let mid = list_data.menu_id;
                let list_eid = item.id;
                detail_tasks.push(tokio::spawn(async move {
                    let detail_result = c.fetch_entry_detail(ds, &l, mid, list_eid).await;
                    (l, detail_result)
                }));
            }
        }
    }

    stats.get_mut("Detail").unwrap().total = total_detail_tasks;
    if total_detail_tasks > 0 {
        log(
            LogLevel::Info,
            &format!("Submitting {} Detail tasks...", total_detail_tasks),
        );
        let mut processed = 0;
        let detail_stream = stream::iter(detail_tasks).buffer_unordered(MAX_DETAIL_CONCUR * 2);

        detail_stream
            .for_each(|res| {
                processed += 1;
                match res {
                    Ok((lang, Ok(Some(data)))) => {
                         let lang_ids = data_store.all_ids.entry(lang.clone()).or_default();
                         lang_ids.insert(data.id);

                         let temp_val = match to_value(&data) {
                            Ok(v) => v,
                            Err(e) => {
                                log(LogLevel::Warning, &format!("Failed to serialize detail for ID collection [{}/{}]: {:?}", lang, data.id, e));
                                Value::Null
                            }
                         };
                         if let Err(e) = collect_nested_ep_ids(&temp_val, lang_ids, 0) {
                            log(LogLevel::Warning, &format!("Failed collecting IDs from detail [{}/{}]: {:?}", lang, data.id, e));
                         }

                        data_store
                            .details
                            .entry(lang)
                            .or_default()
                            .push(data);
                        stats.get_mut("Detail").unwrap().ok += 1;
                    }

                    Ok((_, Ok(None))) => {
                        stats.get_mut("Detail").unwrap().empty += 1;
                    }

                    Ok((lang, Err(e))) => {
                        log(
                            LogLevel::Error,
                            &format!("Detail Task Err [{}]. {:?}", lang, e),
                        );
                        stats.get_mut("Detail").unwrap().fail += 1;
                    }

                    Err(e) => {
                        log(LogLevel::Error, &format!("Detail Task Panic! {}", e));
                        stats.get_mut("Detail").unwrap().fail += 1;
                    }
                }

                if processed % 500 == 0 || processed == total_detail_tasks {
                    let s = stats["Detail"].clone();
                    log(
                        LogLevel::Info,
                        &format!(
                            "Detail progress: {}/{}. ({} OK, {} Empty/Skip, {} Failed)",
                            processed, s.total, s.ok, s.empty, s.fail
                        ),
                    );
                }

                futures::future::ready(())
            })
            .await;

        let detail_s = stats["Detail"].clone();
        log(
            if detail_s.fail == 0 && detail_s.ok > 0 {
                LogLevel::Success
            } else {
                LogLevel::Info
            },
            &format!(
                "Detail phase complete. {} OK, {} Empty/Skip, {} Failed.",
                detail_s.ok, detail_s.empty, detail_s.fail
            ),
        );
    } else {
        log(LogLevel::Info, "No details to fetch.");
    }

    log(
        LogLevel::Step,
        &format!(
            "--- Phase 4: Fetch Calendar ({} langs) ---",
            target_langs.len()
        ),
    );
    let mut cal_tasks = Vec::new();
    stats.get_mut("Calendar").unwrap().total = target_langs.len();
    for lang in target_langs.iter() {
        let c = client.clone();
        let cs = cal_sem.clone();
        let l = lang.clone();
        let vm = vision_maps.get(&l).cloned().unwrap_or_default();
        cal_tasks.push(tokio::spawn(async move {
            let cal_result = c.fetch_calendar(cs, &l, &vm).await;
            (l, cal_result)
        }));
    }

    for handle in cal_tasks {
        match handle.await {
            Ok((lang, Ok(data))) => {
                let lang_ids = data_store.all_ids.entry(lang.clone()).or_default();

                let temp_val = match to_value(&data) {
                    Ok(v) => v,
                    Err(e) => {
                        log(
                            LogLevel::Warning,
                            &format!(
                                "Failed to serialize calendar for ID collection [{}]: {:?}",
                                lang, e
                            ),
                        );
                        Value::Null
                    }
                };
                if let Err(e) = collect_nested_ep_ids(&temp_val, lang_ids, 0) {
                    log(
                        LogLevel::Warning,
                        &format!("Failed collecting IDs from calendar [{}]: {:?}", lang, e),
                    );
                }

                data_store.calendars.insert(lang, data);
                stats.get_mut("Calendar").unwrap().ok += 1;
            }

            Ok((lang, Err(e))) => {
                log(
                    LogLevel::Warning,
                    &format!("Calendar FAIL [{}]. Err: {:?}", lang, e),
                );
                stats.get_mut("Calendar").unwrap().fail += 1;
            }

            Err(e) => {
                log(LogLevel::Error, &format!("Calendar Task Panic! {}", e));
                stats.get_mut("Calendar").unwrap().fail += 1;
            }
        }
    }

    let cal_s = stats["Calendar"].clone();
    log(
        if cal_s.fail == 0 && cal_s.ok > 0 {
            LogLevel::Success
        } else {
            LogLevel::Info
        },
        &format!(
            "Calendar phase complete. {} OK, {} Failed/Empty.",
            cal_s.ok, cal_s.fail
        ),
    );

    log(LogLevel::Step, "--- Phase 5: Centralized Bulk Fetch ---");
    let mut all_bulk_data: AllBulkData = HashMap::new();
    let mut total_ids_to_fetch = 0;
    let all_lang_keys: Vec<String> = data_store.all_ids.keys().cloned().collect();

    for lang in &all_lang_keys {
        if let Some(ids_for_lang) = data_store.all_ids.get(lang) {
            if ids_for_lang.is_empty() {
                continue;
            }

            total_ids_to_fetch += ids_for_lang.len();
            log(
                LogLevel::Info,
                &format!(
                    "Fetching primary bulk data for {} IDs [{}]...",
                    ids_for_lang.len(),
                    lang
                ),
            );
            stats.get_mut("Bulk Fetch").unwrap().total += ids_for_lang.len();

            match client
                .fetch_bulk_data(
                    bulk_sem.clone(),
                    ids_for_lang,
                    lang,
                    &format!("Bulk Primary [{}]", lang),
                )
                .await
            {
                Ok(primary_data) => {
                    stats.get_mut("Bulk Fetch").unwrap().ok += primary_data.len();
                    let mut ids_needing_icon_fallback = HashSet::new();
                    for id in ids_for_lang {
                        let primary_entry = primary_data.get(id);
                        let primary_icon = primary_entry
                            .and_then(|v| v.get("icon_url"))
                            .and_then(|v| v.as_str())
                            .filter(|s| !s.is_empty());
                        if primary_icon.is_none() {
                            ids_needing_icon_fallback.insert(*id);
                        }
                    }

                    let mut fallback_data_map: FallbackBulkDataMap = HashMap::new();
                    if !ids_needing_icon_fallback.is_empty() {
                        log(
                            LogLevel::Info,
                            &format!(
                                "Fetching icon fallbacks for {} IDs [{}]...",
                                ids_needing_icon_fallback.len(),
                                lang
                            ),
                        );
                        let mut lang_tasks = Vec::new();
                        let all_langs_for_fallback: Vec<String> =
                            SUPPORTED_LANGS.iter().cloned().collect();

                        for alt_lang in all_langs_for_fallback.iter() {
                            if alt_lang == lang {
                                continue;
                            }

                            let needed = ids_needing_icon_fallback.clone();
                            let client_clone = client.clone();
                            let sem_clone = bulk_sem.clone();
                            let alt_lang_owned = alt_lang.clone();
                            let ctx_clone = format!("Bulk Icon Alt ({}) for [{}]", alt_lang, lang);

                            lang_tasks.push(tokio::spawn(async move {
                                let result = client_clone
                                    .fetch_bulk_data(
                                        sem_clone,
                                        &needed,
                                        &alt_lang_owned,
                                        &ctx_clone,
                                    )
                                    .await;
                                (alt_lang_owned, result)
                            }));
                        }

                        let results = join_all(lang_tasks).await;
                        for res in results {
                            match res {
                                Ok((l, Ok(bulk_map))) => {
                                    if !bulk_map.is_empty() {
                                        stats.get_mut("Bulk Fetch").unwrap().ok += bulk_map.len();
                                        fallback_data_map.insert(l, bulk_map);
                                    }
                                }

                                Ok((l, Err(e))) => {
                                    log(
                                        LogLevel::Warning,
                                        &format!(
                                            "Bulk Icon Alt ({}) Fetch FAILED for [{}]. Err: {:?}",
                                            l, lang, e
                                        ),
                                    );
                                    stats.get_mut("Bulk Fetch").unwrap().fail +=
                                        ids_needing_icon_fallback.len();
                                }

                                Err(e) => {
                                    log(
                                        LogLevel::Error,
                                        &format!(
                                            "Bulk Icon Alt Task Panic for [{}]. Err: {:?}",
                                            lang, e
                                        ),
                                    );
                                    stats.get_mut("Bulk Fetch").unwrap().fail +=
                                        ids_needing_icon_fallback.len();
                                }
                            }
                        }
                    }

                    all_bulk_data.insert(lang.clone(), (primary_data, fallback_data_map));
                }

                Err(e) => {
                    log(
                        LogLevel::Error,
                        &format!("Primary Bulk Fetch FAIL [{}]. Err: {:?}", lang, e),
                    );
                    stats.get_mut("Bulk Fetch").unwrap().fail += ids_for_lang.len();
                }
            }
        }
    }

    log(
        LogLevel::Info,
        &format!(
            "Total unique IDs across all languages: {}",
            total_ids_to_fetch
        ),
    );
    let bulk_s = stats["Bulk Fetch"].clone();
    log(
        if bulk_s.fail == 0 {
            LogLevel::Success
        } else {
            LogLevel::Warning
        },
        &format!(
            "Bulk Fetch phase complete. {} Fetched, {} Failed (approx).",
            bulk_s.ok, bulk_s.fail
        ),
    );

    log(LogLevel::Step, "--- Updating data in memory ---");
    let mut total_updates = 0;

    for (lang, lists) in data_store.lists.iter_mut() {
        if let Some((primary_bulk, fallback_bulk)) = all_bulk_data.get(lang) {
            for list_file in lists.iter_mut() {
                let list_file_clone = list_file.clone();
                let menu_id = list_file_clone.menu_id;

                match to_value(list_file_clone) {
                    Ok(mut list_value) => {
                        match update_value_with_bulk(
                            &mut list_value,
                            primary_bulk,
                            fallback_bulk,
                            lang,
                            0,
                        ) {
                            Ok(true) => match serde_json::from_value(list_value) {
                                Ok(updated_list) => {
                                    *list_file = updated_list;
                                    total_updates += 1;
                                    stats.get_mut("List").unwrap().updated_in_bulk += 1;
                                }

                                Err(e) => log(
                                    LogLevel::Error,
                                    &format!(
                                        "Failed to deserialize updated List back [{}/{}]: {:?}",
                                        lang, menu_id, e
                                    ),
                                ),
                            },
                            Ok(false) => { /* No changes needed */ }

                            Err(e) => log(
                                LogLevel::Error,
                                &format!("Error updating List [{}/{}]: {:?}", lang, menu_id, e),
                            ),
                        }
                    }

                    Err(e) => log(
                        LogLevel::Error,
                        &format!(
                            "Failed to serialize List for update [{}/{}]: {:?}",
                            lang, menu_id, e
                        ),
                    ),
                }
            }
        }
    }

    for (lang, details) in data_store.details.iter_mut() {
        if let Some((primary_bulk, fallback_bulk)) = all_bulk_data.get(lang) {
            for detail_page in details.iter_mut() {
                let detail_page_clone = detail_page.clone();
                let page_id = detail_page_clone.id;

                match to_value(detail_page_clone) {
                    Ok(mut detail_value) => {
                        match update_value_with_bulk(
                            &mut detail_value,
                            primary_bulk,
                            fallback_bulk,
                            lang,
                            0,
                        ) {
                            Ok(true) => match serde_json::from_value(detail_value) {
                                Ok(updated_detail) => {
                                    *detail_page = updated_detail;
                                    total_updates += 1;
                                    stats.get_mut("Detail").unwrap().updated_in_bulk += 1;
                                }

                                Err(e) => log(
                                    LogLevel::Error,
                                    &format!(
                                        "Failed to deserialize updated Detail back [{}/{}]: {:?}",
                                        lang, page_id, e
                                    ),
                                ),
                            },
                            Ok(false) => { /* No changes needed */ }

                            Err(e) => log(
                                LogLevel::Error,
                                &format!("Error updating Detail [{}/{}]: {:?}", lang, page_id, e),
                            ),
                        }
                    }

                    Err(e) => log(
                        LogLevel::Error,
                        &format!(
                            "Failed to serialize Detail for update [{}/{}]: {:?}",
                            lang, page_id, e
                        ),
                    ),
                }
            }
        }
    }

    for (lang, calendar_file) in data_store.calendars.iter_mut() {
        if let Some((primary_bulk, fallback_bulk)) = all_bulk_data.get(lang) {
            let calendar_file_clone = calendar_file.clone();
            let lang_clone = lang.clone();

            match to_value(calendar_file_clone) {
                Ok(mut calendar_value) => {
                    match update_value_with_bulk(
                        &mut calendar_value,
                        primary_bulk,
                        fallback_bulk,
                        &lang_clone,
                        0,
                    ) {
                        Ok(true) => match serde_json::from_value(calendar_value) {
                            Ok(updated_calendar) => {
                                *calendar_file = updated_calendar;
                                total_updates += 1;
                                stats.get_mut("Calendar").unwrap().updated_in_bulk += 1;
                            }

                            Err(e) => log(
                                LogLevel::Error,
                                &format!(
                                    "Failed to deserialize updated Calendar back [{}]: {:?}",
                                    lang_clone, e
                                ),
                            ),
                        },
                        Ok(false) => { /* No changes needed */ }

                        Err(e) => log(
                            LogLevel::Error,
                            &format!("Error updating Calendar [{}]: {:?}", lang_clone, e),
                        ),
                    }
                }

                Err(e) => log(
                    LogLevel::Error,
                    &format!(
                        "Failed to serialize Calendar for update [{}]: {:?}",
                        lang_clone, e
                    ),
                ),
            }
        }
    }

    log(
        LogLevel::Info,
        &format!("Applied bulk updates to {} data structures.", total_updates),
    );

    log(LogLevel::Step, "--- Phase 6: Saving updated data ---");
    let mut save_tasks = Vec::new();

    for (lang, lists) in data_store.lists {
        for list_file in lists {
            let l = lang.clone();
            let file_name = format!(
                "{}.json",
                crate::utils::clean_filename(&list_file.menu_id.to_string())
            );
            let fpath = dirs["list"].join(&l).join(file_name);
            let ctx = format!("List Menu:{} [{}]", list_file.menu_id, l);
            stats.get_mut("Save").unwrap().total += 1;
            save_tasks.push(tokio::spawn(async move {
                (ctx, save_json(&fpath, list_file, "").await)
            }));
        }
    }

    for (lang, details) in data_store.details {
        for detail_page in details {
            let l = lang.clone();
            let file_name = format!("{}.json", detail_page.id);
            let fpath = dirs["detail"].join(&l).join(file_name);
            let ctx = format!("Detail Entry:{} [{}]", detail_page.id, l);
            stats.get_mut("Save").unwrap().total += 1;
            save_tasks.push(tokio::spawn(async move {
                (ctx, save_json(&fpath, detail_page, "").await)
            }));
        }
    }

    for (lang, calendar_file) in data_store.calendars {
        let l = lang.clone();
        let file_name = format!("{}.json", lang);
        let fpath = dirs["calendar"].join(file_name);
        let ctx = format!("Calendar [{}]", l);
        stats.get_mut("Save").unwrap().total += 1;
        save_tasks.push(tokio::spawn(async move {
            (ctx, save_json(&fpath, calendar_file, "").await)
        }));
    }

    log(
        LogLevel::Info,
        &format!("Saving {} files...", save_tasks.len()),
    );
    let save_stream = stream::iter(save_tasks).buffer_unordered(MAX_DETAIL_CONCUR * 2);
    let mut processed_saves = 0;
    save_stream
        .for_each(|res| {
            processed_saves += 1;
            match res {
                Ok((_ctx, Ok(true))) => {
                    stats.get_mut("Save").unwrap().ok += 1;
                }

                Ok((ctx, Ok(false))) => {
                    log(LogLevel::Error, &format!("Save FAIL (silent) {}", ctx));
                    stats.get_mut("Save").unwrap().fail += 1;
                }

                Ok((ctx, Err(e))) => {
                    log(
                        LogLevel::Error,
                        &format!("Save FAIL (error) {}. Err: {:?}", ctx, e),
                    );
                    stats.get_mut("Save").unwrap().fail += 1;
                }

                Err(e) => {
                    log(LogLevel::Error, &format!("Save Task Panic! {}", e));
                    stats.get_mut("Save").unwrap().fail += 1;
                }
            }

            if processed_saves % 500 == 0 || processed_saves == stats["Save"].total {
                let s = stats["Save"].clone();
                log(
                    LogLevel::Info,
                    &format!(
                        "Save progress: {}/{}. ({} OK, {} Failed)",
                        processed_saves, s.total, s.ok, s.fail
                    ),
                );
            }

            futures::future::ready(())
        })
        .await;

    let save_s = stats["Save"].clone();
    log(
        if save_s.fail == 0 {
            LogLevel::Success
        } else {
            LogLevel::Warning
        },
        &format!(
            "Save phase complete. {} OK, {} Failed.",
            save_s.ok, save_s.fail
        ),
    );

    let duration = start_time.elapsed();
    let sep = "=".repeat(60);
    println!("\n{}\n{:^60}\n{}", sep, "Run Summary", sep);
    println!(
        "Languages Fetched: {} ({})",
        target_langs.len(),
        target_langs.join(", ")
    );
    println!("Total Run Time:    {:.3?}", duration);
    println!("{}", "-".repeat(60));
    println!(
        "{:<15} {:<6} {:<9} {:<6} {:<8} {:<6}",
        "Category", "OK", "Empty/Skip", "Fail", "BulkUpd", "Total"
    );
    println!("{}", "-".repeat(60));

    let mut grand_total_tasks = 0;
    let mut grand_total_ok = 0;
    let mut grand_total_empty = 0;
    let mut grand_total_fail = 0;
    let mut grand_total_updated = 0;
    let mut critical_fail = false;

    for cat in ["Navigation", "List", "Detail", "Calendar"] {
        let s = stats.get(cat).cloned().unwrap_or_default();
        println!(
            "{:<15} {:<6} {:<9} {:<6} {:<8} {:<6}",
            cat, s.ok, s.empty, s.fail, s.updated_in_bulk, s.total
        );
        grand_total_tasks += s.total;
        grand_total_ok += s.ok;
        grand_total_empty += s.empty;
        grand_total_fail += s.fail;
        grand_total_updated += s.updated_in_bulk;

        if s.total > 0 && s.ok == 0 {
            critical_fail = true;
        }
    }

    println!("{}", "-".repeat(60));
    let bulk_s = stats["Bulk Fetch"].clone();
    let save_s = stats["Save"].clone();
    println!(
        "{:<15} {:<6} {:<9} {:<6} {:<8} {:<6}",
        "Bulk Fetch", bulk_s.ok, bulk_s.empty, bulk_s.fail, bulk_s.updated_in_bulk, bulk_s.total
    );
    println!(
        "{:<15} {:<6} {:<9} {:<6} {:<8} {:<6}",
        "Save", save_s.ok, save_s.empty, save_s.fail, save_s.updated_in_bulk, save_s.total
    );

    println!("{}", "-".repeat(60));
    println!(
        "{:<15} {:<6} {:<9} {:<6} {:<8} {:<6}",
        "TOTALS (Fetch)",
        grand_total_ok,
        grand_total_empty,
        grand_total_fail,
        grand_total_updated,
        grand_total_tasks
    );
    println!("{}", sep);

    let exit_code = if critical_fail {
        log(LogLevel::Error, &format!("Run completed with CRITICAL failures in core fetch phase(s). {} OK, {} Failures, {} Skipped/Empty out of {} total tasks.", grand_total_ok, grand_total_fail, grand_total_empty, grand_total_tasks));
        1
    } else if grand_total_fail > 0 || bulk_s.fail > 0 || save_s.fail > 0 {
        log(
            LogLevel::Warning,
            &format!(
                "Run completed with {} task failures, {} bulk fetch failures, and {} save failures.",
                grand_total_fail, bulk_s.fail, save_s.fail
            ),
        );
        0
    } else if grand_total_tasks == 0 && !target_langs.is_empty() {
        log(
            LogLevel::Warning,
            "No fetch tasks were generated or executed for the selected languages.",
        );
        1
    } else if grand_total_ok == 0 && grand_total_tasks > 0 {
        log(
            LogLevel::Error,
            "Run completed, but NO fetch tasks succeeded. All tasks either failed or were skipped/empty.",
        );
        1
    } else {
        log(LogLevel::Success, "Run completed successfully.");
        0
    };

    let end_ts_str = Utc::now().format("%Y-%m-%d %H:%M:%S %Z").to_string();
    log(
        LogLevel::Step,
        &format!("--- Finished at {} ---", end_ts_str),
    );
    Ok(exit_code)
}
