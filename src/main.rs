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
use crate::utils::{run_blocking, save_json};

use chrono::Utc;
use clap::Parser;
use fs_extra::dir::remove;
use futures::stream::{self, StreamExt};
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
                Err(e) => log(
                    LogLevel::Warning,
                    &format!("Failed to remove dir '{}': {:?}", d.display(), e),
                ),
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
    let mut stats = RunStats::new();
    stats.insert("Navigation".into(), Default::default());
    stats.insert("List".into(), Default::default());
    stats.insert("Detail".into(), Default::default());
    stats.insert("Calendar".into(), Default::default());

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
            "Nav phase complete. {} OK, {} FAIL/EMPTY.",
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
            let bs = bulk_sem.clone();
            let l = lang.clone();
            let mid = menu.menu_id;
            let mn = menu.name.clone();
            let ld = dirs["list"].clone();
            list_tasks.push(tokio::spawn(async move {
                fetch_menu_list(c, ls, bs, l, mid, mn, ld).await
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

                    Ok(Err(e)) => match e {
                        AppError::ApiError { retcode, .. } if retcode == 100010 => {
                            stats.get_mut("List").unwrap().empty += 1;
                        }

                        other_err => {
                            log(LogLevel::Error, &format!("List Task Err: {:?}", other_err));
                            stats.get_mut("List").unwrap().fail += 1;
                        }
                    },

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
                            "List progress: {}/{}. ({} OK, {} EMPTY, {} FAIL)",
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
                "List phase complete. {} OK, {} EMPTY, {} FAIL.",
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
            if let Some(vl) = item.filter_values.filters.get("character_vision") {
                if let Some(v) = vl.get(0) {
                    if !v.is_empty() {
                        map.insert(item.entry_page_id, v.clone());
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
            let bs = bulk_sem.clone();
            let l = list_data.language.clone();
            let mid = list_data.menu_id;
            let eid = item.entry_page_id;
            let dd = dirs["detail"].clone();
            detail_tasks.push(tokio::spawn(async move {
                (
                    l.clone(),
                    eid,
                    dd,
                    c.fetch_entry_detail(ds, bs, &l, mid, eid).await,
                )
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
                    Ok((lang, id, dir, Ok(Some(data)))) => {
                        let p = dir.join(&lang).join(format!("{}.json", id));
                        let ctx = format!("Detail Entry:{} [{}]", id, lang);
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

                    Ok((_, _, _, Ok(None))) => {
                        stats.get_mut("Detail").unwrap().empty += 1;
                    }

                    Ok((lang, id, _, Err(e))) => {
                        log(
                            LogLevel::Error,
                            &format!("Detail Task Err [{} Entry:{}]. {:?}", lang, id, e),
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
                            "Detail progress: {}/{}. ({} OK, {} EMPTY, {} FAIL)",
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
            .filter(|res| res.is_err() || res.as_ref().is_ok_and(|inner| inner.is_err()))
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
                "Detail phase complete. {} OK, {} EMPTY, {} FAIL.",
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
        let cs = bulk_sem.clone();
        let l = lang.clone();
        let cd = dirs["calendar"].clone();
        let vm = vision_maps.get(&l).cloned().unwrap_or_default();
        cal_tasks.push(tokio::spawn(async move {
            (l.clone(), cd, c.fetch_calendar(cs, &l, &vm).await)
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

            Ok((lang, _, Err(e))) => {
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
            "Calendar phase complete. {} OK, {} FAIL/EMPTY.",
            cal_s.ok, cal_s.fail
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
        "{:<15} {:<6} {:<6} {:<6} {:<6}",
        "Category", "OK", "Empty", "Fail", "Total"
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
            "{:<15} {:<6} {:<6} {:<6} {:<6}",
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
    println!(
        "{:<15} {:<6} {:<6} {:<6} {:<6}",
        "TOTALS", grand_total_ok, grand_total_empty, grand_total_fail, grand_total_tasks
    );
    println!("{}", sep);

    let actual_failures = grand_total_fail;

    let exit_code = if critical_fail {
        log(
            LogLevel::Error,
            &format!(
                "Run completed with critical failures ({} OK out of {} expected).",
                grand_total_ok,
                grand_total_tasks - grand_total_empty
            ),
        );
        1
    } else if actual_failures > 0 {
        log(
            LogLevel::Warning,
            &format!(
                "Run completed with {} non-critical task failures.",
                actual_failures
            ),
        );
        0
    } else if grand_total_tasks == 0 && !target_langs.is_empty() {
        log(LogLevel::Warning, "No tasks were generated or executed.");
        1
    } else if grand_total_ok == 0 && grand_total_tasks > 0 && grand_total_empty != grand_total_tasks
    {
        log(
            LogLevel::Error,
            "Run completed but no tasks succeeded or were explicitly empty.",
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
