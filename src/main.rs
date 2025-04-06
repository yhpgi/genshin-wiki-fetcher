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
use futures::stream::{self, StreamExt};
use serde_json::{from_str, Value};
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
    files_processed: usize,
    files_updated: usize,
    files_failed: usize,
}

type RunStats = BTreeMap<String, RunCategoryStats>;

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
    let refresh_sem = Arc::new(Semaphore::new(MAX_BULK_CONCUR));
    let mut stats = RunStats::new();
    stats.insert("Navigation".into(), Default::default());
    stats.insert("List".into(), Default::default());
    stats.insert("Detail".into(), Default::default());
    stats.insert("Calendar".into(), Default::default());
    stats.insert("Bulk Refresh".into(), Default::default());

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

    let mut nav_results: HashMap<String, Vec<NavMenuItem>> = HashMap::new();
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
                    if save_json(&path, menus_to_save, &format!("Nav [{}]", lang_clone)).await? {
                        stats.get_mut("Navigation").unwrap().ok += 1;
                        nav_results.insert(lang, menus_data);
                    } else {
                        stats.get_mut("Navigation").unwrap().fail += 1;
                    }
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
    for (lang, menus) in nav_results.iter() {
        for menu in menus {
            total_list_tasks += 1;
            let c = client.clone();
            let ls = list_sem.clone();
            let l = lang.clone();
            let mid = menu.menu_id;
            let mn = menu.name.clone();
            let ld = dirs["list"].clone();
            list_tasks.push(tokio::spawn(async move {
                fetch_menu_list(c, ls, l, mid, mn, ld).await
            }));
        }
    }

    stats.get_mut("List").unwrap().total = total_list_tasks;
    let mut list_results: Vec<OutputListFile> = Vec::new();
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
                        stats.get_mut("List").unwrap().ok += 1;
                        list_results.push(data);
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
        LogLevel::Info,
        "Processing list results for vision mapping...",
    );
    let mut vision_maps: HashMap<String, HashMap<EntryId, String>> = HashMap::new();
    for list_data in &list_results {
        let map = vision_maps.entry(list_data.language.clone()).or_default();
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

    log(LogLevel::Step, "--- Phase 3: Fetch Entry Details ---");
    let mut detail_tasks = Vec::new();
    let mut total_detail_tasks = 0;
    for list_data in &list_results {
        for item in &list_data.list {
            total_detail_tasks += 1;
            let c = client.clone();
            let ds = detail_sem.clone();
            let l = list_data.language.clone();
            let mid = list_data.menu_id;
            let list_eid = item.id;
            let dd = dirs["detail"].clone();
            detail_tasks.push(tokio::spawn(async move {
                let detail_result = c.fetch_entry_detail(ds, &l, mid, list_eid).await;
                (l, list_eid, dd, detail_result)
            }));
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
        let mut save_handles = Vec::new();
        detail_stream
            .for_each(|res| {
                processed += 1;
                match res {
                    Ok((lang, list_eid, dir, Ok(Some(data)))) => {
                        let p = dir.join(&lang).join(format!("{}.json", list_eid));
                        let ctx = format!("Detail Entry:{} [{}]", list_eid, lang);
                        save_handles.push(tokio::spawn(async move {
                            match save_json(&p, data, &ctx).await {
                                Ok(true) => Ok(()),
                                Ok(false) => {
                                    log(
                                        LogLevel::Error,
                                        &format!(
                                            "Detail Save FAIL (silent) {}. File: {}",
                                            ctx,
                                            p.display()
                                        ),
                                    );
                                    Err(())
                                }

                                Err(e) => {
                                    log(
                                        LogLevel::Error,
                                        &format!(
                                            "Detail Save FAIL (error) {}. File: {}. Err: {:?}",
                                            ctx,
                                            p.display(),
                                            e
                                        ),
                                    );
                                    Err(())
                                }
                            }
                        }));
                        stats.get_mut("Detail").unwrap().ok += 1;
                    }

                    Ok((_lang, _list_eid, _dir, Ok(None))) => {
                        stats.get_mut("Detail").unwrap().empty += 1;
                    }

                    Ok((lang, list_eid, _dir, Err(e))) => {
                        log(
                            LogLevel::Error,
                            &format!("Detail Task Err [{} Entry:{}]. {:?}", lang, list_eid, e),
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
        let save_results = futures::future::join_all(save_handles).await;
        let save_failures = save_results
            .into_iter()
            .filter(|res| res.is_err() || res.as_ref().is_ok_and(|inner_res| inner_res.is_err()))
            .count();
        if save_failures > 0 {
            log(
                LogLevel::Warning,
                &format!("{} detail save tasks failed.", save_failures),
            );
        }

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
    let cal_sem = Arc::new(Semaphore::new(5));
    for lang in target_langs.iter() {
        let c = client.clone();
        let cs = cal_sem.clone();
        let l = lang.clone();
        let cd = dirs["calendar"].clone();
        let vm = vision_maps.get(&l).cloned().unwrap_or_default();
        cal_tasks.push(tokio::spawn(async move {
            let cal_result = c.fetch_calendar(cs, &l, &vm).await;
            (l, cd, cal_result)
        }));
    }

    for handle in cal_tasks {
        match handle.await {
            Ok((lang, dir, Ok(data))) => {
                let p = dir.join(format!("{}.json", lang));
                if save_json(&p, data, &format!("Calendar [{}]", lang)).await? {
                    stats.get_mut("Calendar").unwrap().ok += 1;
                } else {
                    stats.get_mut("Calendar").unwrap().fail += 1;
                }
            }

            Ok((lang, _dir, Err(e))) => {
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

    log(LogLevel::Step, "--- Phase 5: Global Bulk Refresh ---");
    let mut files_to_refresh: Vec<(PathBuf, String)> = Vec::new();

    for lang in target_langs.iter() {
        let nav_path = dirs["nav"].join(format!("{}.json", lang));
        if fs::try_exists(&nav_path).await? {
            files_to_refresh.push((nav_path, lang.clone()));
        }

        let cal_path = dirs["calendar"].join(format!("{}.json", lang));
        if fs::try_exists(&cal_path).await? {
            files_to_refresh.push((cal_path, lang.clone()));
        }

        let list_lang_dir = dirs["list"].join(lang);
        if fs::try_exists(&list_lang_dir).await? {
            let mut list_entries = fs::read_dir(list_lang_dir).await?;
            while let Some(entry) = list_entries.next_entry().await? {
                let path = entry.path();
                if path.is_file() && path.extension().map_or(false, |e| e == "json") {
                    files_to_refresh.push((path, lang.clone()));
                }
            }
        }

        let detail_lang_dir = dirs["detail"].join(lang);
        if fs::try_exists(&detail_lang_dir).await? {
            let mut detail_entries = fs::read_dir(detail_lang_dir).await?;
            while let Some(entry) = detail_entries.next_entry().await? {
                let path = entry.path();
                if path.is_file() && path.extension().map_or(false, |e| e == "json") {
                    files_to_refresh.push((path, lang.clone()));
                }
            }
        }
    }

    stats.get_mut("Bulk Refresh").unwrap().total = files_to_refresh.len();
    log(
        LogLevel::Info,
        &format!(
            "Found {} files for potential refresh.",
            files_to_refresh.len()
        ),
    );
    let mut refresh_tasks = Vec::new();

    for (fpath, lang) in files_to_refresh {
        let c = client.clone();
        let rs = refresh_sem.clone();
        let bs = bulk_sem.clone();
        refresh_tasks.push(tokio::spawn(async move {
            scan_and_refresh_file(fpath, lang, c, rs, bs).await
        }));
    }

    let mut files_processed = 0;
    let refresh_stream = stream::iter(refresh_tasks).buffer_unordered(MAX_BULK_CONCUR * 2);
    refresh_stream
        .for_each(|res| {
            files_processed += 1;
            match res {
                Ok(Ok(true)) => {
                    stats.get_mut("Bulk Refresh").unwrap().files_updated += 1;
                }

                Ok(Ok(false)) => {}

                Ok(Err(e)) => {
                    log(LogLevel::Error, &format!("Bulk Refresh Task Err: {:?}", e));
                    stats.get_mut("Bulk Refresh").unwrap().files_failed += 1;
                }

                Err(e) => {
                    log(LogLevel::Error, &format!("Bulk Refresh Task Panic! {}", e));
                    stats.get_mut("Bulk Refresh").unwrap().files_failed += 1;
                }
            }

            if files_processed % 500 == 0 || files_processed == stats["Bulk Refresh"].total {
                let s = stats["Bulk Refresh"].clone();
                log(
                    LogLevel::Info,
                    &format!(
                        "Bulk Refresh progress: {}/{}. ({} Updated, {} Failed)",
                        files_processed, s.total, s.files_updated, s.files_failed
                    ),
                );
            }

            futures::future::ready(())
        })
        .await;

    let refresh_s = stats["Bulk Refresh"].clone();
    log(
        if refresh_s.files_failed == 0 {
            LogLevel::Success
        } else {
            LogLevel::Warning
        },
        &format!(
            "Bulk Refresh phase complete. {} Files Processed, {} Updated, {} Failed.",
            refresh_s.total, refresh_s.files_updated, refresh_s.files_failed
        ),
    );

    let duration = start_time.elapsed();
    let sep = "=".repeat(53);
    println!("\n{}\n{:^53}\n{}", sep, "Run Summary", sep);
    println!(
        "Languages Fetched: {} ({})",
        target_langs.len(),
        target_langs.join(", ")
    );
    println!("Total Run Time:    {:.3?}", duration);
    println!("{}", "-".repeat(53));
    println!(
        "{:<15} {:<6} {:<9} {:<6} {:<6}",
        "Category", "OK", "Empty/Skip", "Fail", "Total"
    );
    println!("{}", "-".repeat(53));

    let mut grand_total_tasks = 0;
    let mut grand_total_ok = 0;
    let mut grand_total_empty = 0;
    let mut grand_total_fail = 0;
    let mut critical_fail = false;

    for cat in ["Navigation", "List", "Detail", "Calendar"] {
        let s = stats.get(cat).cloned().unwrap_or_default();
        println!(
            "{:<15} {:<6} {:<9} {:<6} {:<6}",
            cat, s.ok, s.empty, s.fail, s.total
        );
        grand_total_tasks += s.total;
        grand_total_ok += s.ok;
        grand_total_empty += s.empty;
        grand_total_fail += s.fail;

        if s.total > 0 && s.ok == 0 && (cat == "Navigation" || cat == "List" || cat == "Detail") {
            critical_fail = true;
        }
    }

    println!("{}", "-".repeat(53));
    let rf_s = stats["Bulk Refresh"].clone();
    println!(
        "{:<15} {:<6} {:<9} {:<6} {:<6}",
        "Bulk Refresh",
        rf_s.files_updated,
        rf_s.total - rf_s.files_updated - rf_s.files_failed,
        rf_s.files_failed,
        rf_s.total
    );
    println!(
        "{:<15} {:<6} {:<9} {:<6} {:<6}",
        "Files Proc.", rf_s.files_processed, "", "", ""
    );

    println!("{}", "-".repeat(53));
    println!(
        "{:<15} {:<6} {:<9} {:<6} {:<6}",
        "TOTALS", grand_total_ok, grand_total_empty, grand_total_fail, grand_total_tasks
    );
    println!("{}", sep);

    let exit_code = if critical_fail {
        log(LogLevel::Error, &format!("Run completed with CRITICAL failures in core phase(s). {} OK, {} Failures, {} Skipped/Empty out of {} total tasks.", grand_total_ok, grand_total_fail, grand_total_empty, grand_total_tasks));
        1
    } else if grand_total_fail > 0 || rf_s.files_failed > 0 {
        log(
            LogLevel::Warning,
            &format!(
                "Run completed with {} task failures and {} refresh failures.",
                grand_total_fail, rf_s.files_failed
            ),
        );
        0
    } else if grand_total_tasks == 0 && !target_langs.is_empty() {
        log(
            LogLevel::Warning,
            "No tasks were generated or executed for the selected languages.",
        );
        1
    } else if grand_total_ok == 0 && grand_total_tasks > 0 {
        log(
            LogLevel::Error,
            "Run completed, but NO tasks succeeded. All tasks either failed or were skipped/empty.",
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

async fn scan_and_refresh_file(
    fpath: PathBuf,
    lang: String,
    client: Arc<ApiClient>,
    refresh_sem: Arc<Semaphore>,
    bulk_sem: Arc<Semaphore>,
) -> AppResult<bool> {
    let _permit = refresh_sem
        .acquire()
        .await
        .map_err(|e| AppError::Unexpected(format!("Refresh semaphore acquire failed: {}", e)))?;
    let log_ctx = format!(
        "Refresh File [{}] {}",
        lang,
        fpath.file_name().unwrap_or_default().to_string_lossy()
    );

    let content = fs::read_to_string(&fpath).await?;
    let mut value: Value = from_str(&content).map_err(AppError::SerdeJsonSerialize)?;

    let mut ids_to_refresh = HashSet::new();
    collect_nested_ep_ids(&value, &mut ids_to_refresh, 0)?;

    if ids_to_refresh.is_empty() {
        return Ok(false);
    }

    let primary_bulk_data = client
        .fetch_bulk_data(
            bulk_sem.clone(),
            &ids_to_refresh,
            &lang,
            &format!("{} Primary", log_ctx),
        )
        .await?;

    let mut fallback_bulk_data_map: HashMap<String, HashMap<EntryId, Value>> = HashMap::new();
    let mut ids_needing_icon_fallback = HashSet::new();

    for id in &ids_to_refresh {
        let primary_entry = primary_bulk_data.get(id);
        let primary_icon = primary_entry
            .and_then(|v| v.get("icon_url"))
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty());
        if primary_icon.is_none() {
            ids_needing_icon_fallback.insert(*id);
        }
    }

    if !ids_needing_icon_fallback.is_empty() {
        let all_langs: Vec<String> = SUPPORTED_LANGS.iter().cloned().collect();
        let mut lang_tasks = Vec::new();
        for alt_lang in all_langs.iter() {
            if alt_lang == &lang {
                continue;
            }

            let needed = ids_needing_icon_fallback.clone();
            let client_clone = client.clone();
            let sem_clone = bulk_sem.clone();
            let alt_lang_owned = alt_lang.clone();
            let ctx_clone = format!("{} Icon Alt ({})", log_ctx, alt_lang);

            lang_tasks.push(tokio::spawn(async move {
                let result = client_clone
                    .fetch_bulk_data(sem_clone, &needed, &alt_lang_owned, &ctx_clone)
                    .await;
                (alt_lang_owned, result)
            }));
        }

        let results = futures::future::join_all(lang_tasks).await;
        for res in results {
            match res {
                Ok((l, Ok(bulk_map))) => {
                    fallback_bulk_data_map.insert(l, bulk_map);
                }

                Ok((l, Err(e))) => {
                    log(
                        LogLevel::Warning,
                        &format!("{} Icon Alt ({}) Fetch FAILED. Err: {:?}", log_ctx, l, e),
                    );
                }

                Err(e) => {
                    log(
                        LogLevel::Error,
                        &format!("{} Icon Alt Task Panic! Err: {:?}", log_ctx, e),
                    );
                }
            }
        }
    }

    let modified = update_value_with_bulk(
        &mut value,
        &primary_bulk_data,
        &fallback_bulk_data_map,
        &lang,
        0,
    )?;

    if modified {
        if save_json(&fpath, value.clone(), &log_ctx).await? {
            Ok(true)
        } else {
            Err(AppError::Processing(format!(
                "Failed to save updated file: {}",
                fpath.display()
            )))
        }
    } else {
        Ok(false)
    }
}
