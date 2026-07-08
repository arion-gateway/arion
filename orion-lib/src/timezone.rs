use crate::Result;
use atomicoption::AtomicOption;
use chrono::Utc;
use chrono_tz::Tz;
use orion_configuration::config::timezone::TimeZone;
use std::str::FromStr;
use std::sync::atomic::Ordering;
use tokio::task::JoinSet;
use tracing::info;

static LOCAL_OFFSET_SEC: AtomicOption<i32> = AtomicOption::none();

pub fn local_offset_sec() -> Option<i32> {
    LOCAL_OFFSET_SEC.as_ref(Ordering::Acquire).copied()
}

#[derive(Debug, thiserror::Error)]
pub enum TimeZoneError {
    #[error("Invalid timezone: {0}")]
    InvalidTimeZone(#[from] chrono_tz::ParseError),
}

use chrono::{Datelike, TimeZone as ChronoTimeZone};

pub fn observes_daylight_saving(tz: &Tz) -> bool {
    let now = Utc::now();
    let year = now.year();

    let mut first_offset = None;
    for month in 1..=12 {
        if let Some(dt) = tz.with_ymd_and_hms(year, month, 1, 0, 0, 0).single() {
            let offset_sec = dt.naive_local().signed_duration_since(dt.naive_utc()).num_seconds() as i32;
            if let Some(first) = first_offset {
                if first != offset_sec {
                    return true;
                }
            } else {
                first_offset = Some(offset_sec);
            }
        }
    }
    false
}

pub async fn init_tz_cache(set: &mut JoinSet<Result<()>>, tz: &TimeZone) -> Result<()> {
    let tz_local = tz.local.to_uppercase();
    if tz_local.is_empty() || tz_local == "UTC" {
        return Ok(());
    }

    info!("Initializing local timezone for {}...", tz.local);

    let tz = Tz::from_str(&tz.local)?;

    if !observes_daylight_saving(&tz) {
        info!("Timezone {} has constant offset, setting it once", tz.name());
        let now = Utc::now().with_timezone(&tz);
        let offset_sec = now.naive_local().signed_duration_since(now.naive_utc()).num_seconds() as i32;
        LOCAL_OFFSET_SEC.store(Ordering::Release, offset_sec);
        orion_format::context::set_local_offset_sec(offset_sec);
        return Ok(());
    }

    info!("Launching background task for DLS timezone {}", tz.name());

    set.spawn(async move {
        let mut prev_offset = 0;
        loop {
            let now = Utc::now().with_timezone(&tz);

            // Calculate offset
            let offset_sec = now.naive_local().signed_duration_since(now.naive_utc()).num_seconds() as i32;

            if prev_offset != offset_sec {
                prev_offset = offset_sec;
                LOCAL_OFFSET_SEC.store(Ordering::Release, offset_sec);
                orion_format::context::set_local_offset_sec(offset_sec);
            }

            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        }
    });

    Ok(())
}
