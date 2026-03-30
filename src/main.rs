use chrono::{Datelike, Duration, Local, NaiveDate, NaiveDateTime, Timelike};
use clap::{Parser, ValueEnum};
use ical::parser::ical::component::IcalEvent;
use ical::IcalParser;
use serde::Serialize;
use std::collections::{BTreeMap, HashSet};
use std::fs::File;
use std::io::{self, BufReader, Write};
use std::path::{Path, PathBuf};

const VERSION: &str = env!("CARGO_PKG_VERSION");

// Terminal colors - automatically disabled if not a TTY
mod color {
    use std::io::IsTerminal;
    use std::fmt;

    pub fn is_color_enabled() -> bool {
        std::io::stdout().is_terminal()
    }

    #[derive(Clone, Copy)]
    pub struct Color(u8);

    impl Color {
        pub fn display(&self) -> impl fmt::Display + '_ {
            if is_color_enabled() {
                format!("\x1b[{}m", self.0)
            } else {
                String::new()
            }
        }
    }

    impl fmt::Display for Color {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "{}", self.display())
        }
    }

    pub const CYAN: Color = Color(36);
    pub const GREEN: Color = Color(32);
    pub const YELLOW: Color = Color(33);
    pub const MAGENTA: Color = Color(35);
    pub const BOLD: Color = Color(1);
}

fn colored<S: AsRef<str>>(c: color::Color, text: S) -> String {
    format!("{}{}{}", c, text.as_ref(), c)
}

#[derive(Debug, Clone, ValueEnum)]
enum DateFilter {
    Current,
    Previous,
    All,
    Today,
    Yesterday,
    Week,
}

#[derive(Debug, Clone, ValueEnum)]
enum OutputFormat {
    Text,
    Json,
    Csv,
    Markdown,
    Ical,
    Html,
}

#[derive(Parser, Debug)]
#[command(name = "proton-extractor", about = "Sum calendar event hours from ICS files", version = VERSION)]
struct Args {
    /// Paths to .ics files
    files: Vec<PathBuf>,

    /// Filter by date: current month, previous, all, or today
    #[arg(short, long, value_enum, default_value = "all")]
    date: DateFilter,

    /// Filter by a specific year (e.g., 2024)
    #[arg(long)]
    year: Option<i32>,

    /// Filter by a specific month (1-12, requires --year)
    #[arg(long, requires = "year")]
    month: Option<u32>,

    /// Only show totals, hide individual events
    #[arg(short, long)]
    quiet: bool,

    /// Output format
    #[arg(short, long, value_enum, default_value = "text")]
    format: OutputFormat,

    /// Exclude events matching this person name (case-insensitive, can be repeated)
    #[arg(long)]
    exclude_person: Vec<String>,

    /// Filter by person name (case-insensitive)
    #[arg(long)]
    person: Option<String>,

    /// Filter by project name in {project} tags (case-insensitive)
    #[arg(long)]
    project: Option<String>,

    /// Exclude events matching this project name in {project} tags (case-insensitive, can be repeated)
    #[arg(long)]
    exclude_project: Vec<String>,

    /// Start date (YYYY-MM-DD)
    #[arg(long, alias = "since")]
    from: Option<NaiveDate>,

    /// End date (YYYY-MM-DD)
    #[arg(long, alias = "until")]
    to: Option<NaiveDate>,

    /// Enable verbose output
    #[arg(short = 'v', long)]
    verbose: bool,

    /// Only show total hours, hide per-person breakdown
    #[arg(long)]
    sum_only: bool,

    /// Output file path (default: stdout)
    #[arg(short, long)]
    output: Option<PathBuf>,

    /// List all unique persons found in events
    #[arg(long)]
    list_persons: bool,

    /// List all unique projects found in events
    #[arg(long)]
    list_projects: bool,

    /// List all unique events found (one per line with date and summary)
    #[arg(long)]
    list_events: bool,

    /// Preview mode: show event count without processing output
    #[arg(long)]
    dry_run: bool,

    /// Filter by day of week: MO,TU,WE,TH,FR,SA,SU (can be repeated, e.g., --weekdays MO --weekdays WE)
    #[arg(long, value_delimiter = ',', value_name = "DAYS")]
    weekdays: Option<Vec<String>>,

    /// Exclude events on these days of week: MO,TU,WE,TH,FR,SA,SU (can be repeated, complements --weekdays)
    #[arg(long, value_delimiter = ',', value_name = "DAYS")]
    exclude_weekdays: Option<Vec<String>>,

    /// Exclude events whose summary contains this text (case-insensitive, can be repeated)
    #[arg(long)]
    exclude_summary: Vec<String>,

    /// Enable compact JSON output (no pretty-printing)
    #[arg(long, requires = "format")]
    compact: bool,

    /// Show statistics about events (count, avg/day, top person, busiest day)
    #[arg(long)]
    stats: bool,

    /// Reverse chronological order (newest first)
    #[arg(long)]
    reverse: bool,

    /// Limit output to N events (useful for large datasets)
    #[arg(long)]
    limit: Option<usize>,

    /// Quick filter: show only today's events
    #[arg(long)]
    today: bool,

    /// Quick filter: show only yesterday's events
    #[arg(long)]
    yesterday: bool,

    /// Filter out events shorter than this duration (e.g., "30m", "1h", "2h30m")
    #[arg(long)]
    min_duration: Option<String>,

    /// Filter out events longer than this duration (e.g., "8h", "4h30m")
    #[arg(long)]
    max_duration: Option<String>,
}

fn validate_date_range(from: &Option<NaiveDate>, to: &Option<NaiveDate>) -> io::Result<()> {
    if let (Some(from_date), Some(to_date)) = (from, to) {
        if from_date > to_date {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("--from ({}) must be before or equal to --to ({})", from_date, to_date),
            ));
        }
    }
    Ok(())
}

fn validate_month(month: Option<u32>) -> io::Result<()> {
    if let Some(m) = month {
        if !(1..=12).contains(&m) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("--month must be between 1 and 12, got {}", m),
            ));
        }
    }
    Ok(())
}

fn validate_ics_file(path: &Path) -> io::Result<()> {
    let extension = path
        .extension()
        .and_then(|e| e.to_str())
        .map(|e| e.to_lowercase());
    
    match extension.as_deref() {
        Some("ics") => Ok(()),
        Some(ext) => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("File '{}' has invalid extension '.{}'. Expected '.ics' file", path.display(), ext),
        )),
        None => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("File '{}' has no file extension. Expected '.ics' file", path.display()),
        )),
    }
}

#[derive(Clone)]
struct Event {
    summary: String,
    start: NaiveDateTime,
    end: NaiveDateTime,
}

impl Event {
    fn new(summary: String, start: NaiveDateTime, end: NaiveDateTime) -> Self {
        Self { summary, start, end }
    }
}

struct RawEvent {
    summary: String,
    start: NaiveDateTime,
    end: NaiveDateTime,
    uid: String,
    rrule: Option<String>,
    exdates: Vec<NaiveDate>,
    recurrence_id: Option<NaiveDateTime>,
}

fn parse_ical_datetime(value: &str) -> Option<NaiveDateTime> {
    // Handle UTC suffix
    let value = value.trim_end_matches('Z');
    
    // Handle UTC offset suffix (e.g., +0530, -0800, +00:00)
    let (clean, _offset_minutes) = if let Some(idx) = value.rfind(|c| ['+', '-'].contains(&c)) {
        if idx > 0 {
            let offset_str = &value[idx + 1..];
            // Only process if offset looks valid (4 or 5 digits like 0530 or +05:30)
            if offset_str.len() >= 4 {
                let offset_clean = offset_str.replace(':', "");
                if offset_clean.chars().all(|c| c.is_ascii_digit()) {
                    let sign = if value.chars().nth(idx) == Some('-') { -1 } else { 1 };
                    let offset_hhmm = offset_clean[..4].parse::<i32>().ok()?;
                    let offset_mins = sign * ((offset_hhmm / 100) * 60 + (offset_hhmm % 100));
                    return NaiveDateTime::parse_from_str(&value[..idx], "%Y%m%dT%H%M%S")
                        .ok()
                        .map(|dt| dt - Duration::minutes(offset_mins.into()));
                }
            }
        }
        (value, 0)
    } else {
        (value, 0)
    };
    
    NaiveDateTime::parse_from_str(clean, "%Y%m%dT%H%M%S")
        .or_else(|_| {
            NaiveDate::parse_from_str(clean, "%Y%m%d")
                .map(|d| d.and_hms_opt(0, 0, 0).unwrap())
        })
        .ok()
}

fn parse_duration(duration: &str) -> Option<Duration> {
    // Parse ISO 8601 duration format: P[n]D, P[n]W, PT[n]H, PT[n]M, etc.
    let duration = duration.trim();
    if duration.is_empty() || !duration.starts_with('P') {
        return None;
    }
    
    let mut days: i64 = 0;
    let mut weeks: i64 = 0;
    let mut hours: i64 = 0;
    let mut minutes: i64 = 0;
    
    let mut num_str = String::new();
    let mut has_unit = false;
    let mut after_t = false;
    
    for ch in duration.chars().skip(1) { // Skip 'P'
        match ch {
            'D' => {
                if let Ok(n) = num_str.parse() { days = n; has_unit = true; }
                num_str.clear();
            }
            'W' => {
                if let Ok(n) = num_str.parse() { weeks = n; has_unit = true; }
                num_str.clear();
            }
            'T' => {
                after_t = true;
                continue;
            }
            'H' if after_t => {
                if let Ok(n) = num_str.parse() { hours = n; has_unit = true; }
                num_str.clear();
            }
            'M' if after_t => {
                if let Ok(n) = num_str.parse() { minutes = n; has_unit = true; }
                num_str.clear();
            }
            '0'..='9' => num_str.push(ch),
            _ => {}
        }
    }
    
    // Must have at least one unit
    if !has_unit {
        return None;
    }
    
    Some(Duration::days(days) + Duration::weeks(weeks) + Duration::hours(hours) + Duration::minutes(minutes))
}

/// Parse a human-readable duration string like "30m", "1h", "2h30m", "1d"
pub fn parse_human_duration(s: &str) -> Option<Duration> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }

    let mut total = Duration::zero();
    let mut current_num = String::new();

    for ch in s.chars() {
        match ch {
            '0'..='9' => current_num.push(ch),
            'h' | 'H' => {
                if let Ok(n) = current_num.parse() {
                    total = total + Duration::hours(n);
                }
                current_num.clear();
            }
            'm' | 'M' => {
                if let Ok(n) = current_num.parse() {
                    total = total + Duration::minutes(n);
                }
                current_num.clear();
            }
            'd' | 'D' => {
                if let Ok(n) = current_num.parse() {
                    total = total + Duration::days(n);
                }
                current_num.clear();
            }
            'w' | 'W' => {
                if let Ok(n) = current_num.parse() {
                    total = total + Duration::weeks(n);
                }
                current_num.clear();
            }
            ' ' | '\t' => {} // ignore whitespace
            _ => return None, // invalid character
        }
    }

    // Handle trailing number without unit (treat as minutes)
    if !current_num.is_empty() {
        if let Ok(n) = current_num.parse() {
            total = total + Duration::minutes(n);
        } else {
            return None;
        }
    }

    if total.num_minutes() > 0 {
        Some(total)
    } else {
        None
    }
}

type RRuleParseResult = (String, NaiveDateTime, Option<Vec<String>>, Option<i32>, Option<i32>);

fn parse_rrule(rrule: &str) -> Option<RRuleParseResult> {
    let mut freq = None;
    let mut until = None;
    let mut byday = None;
    let mut interval: Option<i32> = None;
    let mut count: Option<i32> = None;
    for part in rrule.split(';') {
        if let Some(v) = part.strip_prefix("FREQ=") {
            freq = Some(v.to_string());
        } else if let Some(v) = part.strip_prefix("UNTIL=") {
            until = parse_ical_datetime(v);
        } else if let Some(v) = part.strip_prefix("BYDAY=") {
            byday = Some(v.split(',').map(|s| s.to_string()).collect());
        } else if let Some(v) = part.strip_prefix("INTERVAL=") {
            interval = v.parse().ok().filter(|&i| i > 0);
        } else if let Some(v) = part.strip_prefix("COUNT=") {
            count = v.parse().ok().filter(|&c| c > 0);
        }
    }
    // Use a far-future datetime as default (guaranteed valid since year 2099 is always valid)
    let default_until = NaiveDate::from_ymd_opt(2099, 12, 31)
        .expect("Date 2099-12-31 should always be valid")
        .and_hms_opt(23, 59, 59)
        .expect("Time 23:59:59 should always be valid");
    Some((freq?, until.unwrap_or(default_until), byday, interval, count))
}

const RECURRENCE_LIMIT_DAYS: i64 = 365 * 5; // 5 year limit for recurrence expansion
const MAX_RECURRENCE_INSTANCES: usize = 365; // Safety limit for instances

fn expand_events(raw_events: Vec<RawEvent>) -> Vec<Event> {
    // Separate overrides (events with RECURRENCE-ID) from base events
    let mut overrides: HashSet<(String, NaiveDate)> = HashSet::new();
    let mut override_events: Vec<RawEvent> = Vec::new();
    let mut base_events: Vec<RawEvent> = Vec::new();

    for event in raw_events {
        if let Some(rid) = event.recurrence_id {
            overrides.insert((event.uid.clone(), rid.date()));
            override_events.push(event);
        } else {
            base_events.push(event);
        }
    }

    let mut result: Vec<Event> = Vec::new();

    // Add override events directly (filter invalid durations)
    result.extend(override_events.into_iter().filter_map(|e| {
        let duration = e.end - e.start;
        if duration.num_minutes() > 0 {
            Some(Event::new(e.summary, e.start, e.end))
        } else {
            None
        }
    }));

    // Expand base events
    for event in base_events {
        let exdate_set: HashSet<NaiveDate> = event.exdates.into_iter().collect();

        match &event.rrule {
            None => {
                let duration = event.end - event.start;
                if duration.num_minutes() > 0 {
                    result.push(Event::new(event.summary, event.start, event.end));
                }
            }
            Some(rrule) => {
                let Some((freq, until, byday, interval, count)) = parse_rrule(rrule) else {
                    // Can't parse RRULE, just add the single event
                    let duration = event.end - event.start;
                    if duration.num_minutes() > 0 {
                        result.push(Event::new(event.summary, event.start, event.end));
                    }
                    continue;
                };

                let duration = event.end - event.start;
                if duration.num_minutes() <= 0 {
                    continue;
                }

                // INTERVAL defaults to 1 if not specified
                let step = match freq.as_str() {
                    "WEEKLY" => Duration::weeks(interval.unwrap_or(1) as i64),
                    "DAILY" => Duration::days(interval.unwrap_or(1) as i64),
                    "MONTHLY" => Duration::days(0), // Placeholder - handled separately
                    "YEARLY" => Duration::days(0),  // Placeholder - handled separately
                    _ => {
                        // Unsupported frequency, add single event
                        result.push(Event::new(event.summary, event.start, event.end));
                        continue;
                    }
                };

                // Clamp until to avoid unbounded expansion
                let start_date = event.start.date();
                let limit_date = start_date.and_hms_opt(23, 59, 59).unwrap().and_utc().naive_local() + Duration::days(RECURRENCE_LIMIT_DAYS);
                let until = if until > limit_date { limit_date } else { until };

                // For MONTHLY recurrence, track original day to maintain consistency
                let original_day = event.start.day();
                let mut current = event.start;
                let mut instances = 0;
                while current <= until {
                    let max_instances = count.unwrap_or(MAX_RECURRENCE_INSTANCES as i32) as usize;
                    if instances >= max_instances {
                        break;
                    }
                    let date = current.date();
                    
                    // BYDAY filter: only include if no BYDAY specified or date matches one of the days
                    let include_byday = byday.as_ref().is_none_or(|days| {
                        days.iter().any(|d| {
                            weekday_abbrev_to_num(d)
                                .map(|wd| date.weekday().num_days_from_monday() + 1 == wd)
                                .unwrap_or(false)
                        })
                    });
                    
                    if include_byday
                        && !exdate_set.contains(&date)
                        && !overrides.contains(&(event.uid.clone(), date))
                    {
                        result.push(Event::new(event.summary.clone(), current, current + duration));
                    }

                    // Increment to next occurrence
                    if freq == "MONTHLY" {
                        // Increment by one month, keeping same day/time
                        let (year, month) = (current.year(), current.month());
                        let (new_year, new_month) = if month == 12 {
                            (year + 1, 1)
                        } else {
                            (year, month + 1)
                        };
                        // Days in each month
                        let days_in_month = match new_month {
                            1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
                            4 | 6 | 9 | 11 => 30,
                            2 => {
                                // Check for leap year
                                if (new_year % 4 == 0 && new_year % 100 != 0) || (new_year % 400 == 0) {
                                    29
                                } else {
                                    28
                                }
                            }
                            _ => 31,
                        };
                        // Use original day (clamped to max days in target month)
                        let new_day = original_day.min(days_in_month);
                        if let Some(new_date) = NaiveDate::from_ymd_opt(new_year, new_month, new_day) {
                            current = new_date.and_hms_opt(current.hour(), current.minute(), current.second()).unwrap_or(current);
                        } else {
                            // Fallback: shouldn't happen with our day calculation
                            current += Duration::days(30);
                        }
                    } else if freq == "YEARLY" {
                        // Increment by one year
                        let new_year = current.year() + 1;
                        // Get the days in the target month/year to handle leap years
                        let days_in_target_month = match current.month() {
                            1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
                            4 | 6 | 9 | 11 => 30,
                            2 => {
                                if (new_year % 4 == 0 && new_year % 100 != 0) || (new_year % 400 == 0) {
                                    29
                                } else {
                                    28
                                }
                            }
                            _ => 31,
                        };
                        // Clamp original day to valid days in target month
                        let new_day = original_day.min(days_in_target_month);
                        if let Some(new_date) = NaiveDate::from_ymd_opt(new_year, current.month(), new_day) {
                            current = new_date.and_hms_opt(current.hour(), current.minute(), current.second()).unwrap_or(current);
                        } else {
                            // Fallback: shouldn't happen with proper day calculation
                            current += Duration::days(365);
                        }
                    } else {
                        current += step;
                    }
                    instances += 1;
                }
            }
        }
    }

    result.sort_by_key(|e| e.start);
    result
}

fn extract_raw_events(ical_events: Vec<IcalEvent>) -> Vec<RawEvent> {
    ical_events
        .into_iter()
        .filter_map(|e| {
            let mut summary = String::from("(untitled)");
            let mut start = None;
            let mut end = None;
            let mut duration = None;
            let mut uid = String::new();
            let mut rrule = None;
            let mut exdates = Vec::new();
            let mut recurrence_id = None;

            for prop in &e.properties {
                let val = match &prop.value {
                    Some(v) => v.as_str(),
                    None => continue,
                };
                match prop.name.as_str() {
                    "SUMMARY" => summary = val.to_string(),
                    "DTSTART" => start = parse_ical_datetime(val),
                    "DTEND" => end = parse_ical_datetime(val),
                    "DURATION" => duration = parse_duration(val),
                    "UID" => uid = val.to_string(),
                    "RRULE" => rrule = Some(val.to_string()),
                    "EXDATE" => {
                        if let Some(dt) = parse_ical_datetime(val) {
                            exdates.push(dt.date());
                        }
                    }
                    "RECURRENCE-ID" => recurrence_id = parse_ical_datetime(val),
                    _ => {}
                }
            }

            let start = start?;
            
            // If DTEND is missing but DURATION is present, compute end time
            let end = match end {
                Some(e) => e,
                None => {
                    let dur = duration?;
                    start + dur
                }
            };

            Some(RawEvent {
                summary,
                start,
                end,
                uid,
                rrule,
                exdates,
                recurrence_id,
            })
        })
        .collect()
}

/// Helper struct for computing month summary
struct MonthSummary {
    month_name: String,
    events: Vec<Event>,
}

impl MonthSummary {
    fn new(year: i32, month: u32, events: Vec<Event>) -> Self {
        let month_name = chrono::Month::try_from(u8::try_from(month).unwrap_or(1))
            .unwrap_or(chrono::Month::January)
            .name()
            .to_string();
        let _ = year; // used in debug assertions if any
        Self { month_name, events }
    }

    fn total_minutes(&self) -> i64 {
        self.events.iter().filter_map(event_duration_minutes).sum()
    }

    fn by_person(&self) -> BTreeMap<String, i64> {
        let mut map: BTreeMap<String, i64> = BTreeMap::new();
        for event in &self.events {
            if let Some(mins) = event_duration_minutes(event) {
                let person = extract_person(&event.summary).unwrap_or("(unknown)");
                *map.entry(person.to_string()).or_default() += mins;
            }
        }
        map
    }

    fn write_html(&self, out: &mut dyn Write, quiet: bool, sum_only: bool, grand_total: i64) -> io::Result<()> {
        let month_total = self.total_minutes();
        writeln!(out, "  <div class=\"month-section\">")?;
        writeln!(out, "    <h2>📅 {}</h2>", self.month_name)?;
        if !quiet && !sum_only {
            writeln!(out, "    <ul class=\"event-list\">")?;
            for event in &self.events {
                if let Some(mins) = event_duration_minutes(event) {
                    writeln!(out, "      <li class=\"event-item\">")?;
                    writeln!(out, "        <span class=\"summary\">{}</span>", html_escape(&event.summary))?;
                    writeln!(out, "        <span class=\"duration\">{}</span>", format_hours(mins))?;
                    writeln!(out, "      </li>")?;
                }
            }
            writeln!(out, "    </ul>")?;
        }
        writeln!(out, "    <div class=\"person-breakdown\">")?;
        writeln!(out, "      <h3>👤 By Person</h3>")?;
        for (person, mins) in self.by_person() {
            let pct = format_percentage(mins, month_total);
            writeln!(out, "      <div class=\"person-summary\">")?;
            writeln!(out, "        <span>{}</span>", html_escape(&person))?;
            writeln!(out, "        <span><strong>{}</strong> <span class=\"percentage\">({})</span></span>", format_hours(mins), pct)?;
            writeln!(out, "      </div>")?;
        }
        writeln!(out, "    </div>")?;
        writeln!(out, "    <div class=\"total\">")?;
        writeln!(out, "      📊 Total: {} ({:.1}%)", format_hours(month_total), (month_total as f64 / grand_total as f64) * 100.0)?;
        writeln!(out, "    </div>")?;
        writeln!(out, "  </div>")?;
        Ok(())
    }
}

/// Groups events by year-month, sorted chronologically
fn group_by_month(events: &[&Event]) -> BTreeMap<(i32, u32), MonthSummary> {
    let mut by_month: BTreeMap<(i32, u32), Vec<Event>> = BTreeMap::new();
    for event in events {
        let key = (event.start.year(), event.start.month());
        by_month.entry(key).or_default().push((*event).clone());
    }
    by_month.into_iter()
        .map(|((year, month), evs)| ((year, month), MonthSummary::new(year, month, evs)))
        .collect()
}

fn matches_filter(event: &Event, filter: &DateFilter) -> bool {
    let now = Local::now().naive_local();
    let yesterday = now - Duration::days(1);
    let (ev_year, ev_month, ev_day) = (event.start.year(), event.start.month(), event.start.day());
    match filter {
        DateFilter::All => true, // Show all events regardless of date
        DateFilter::Current => ev_year == now.year() && ev_month == now.month(),
        DateFilter::Previous => {
            let (y, m) = if now.month() == 1 {
                (now.year() - 1, 12)
            } else {
                (now.year(), now.month() - 1)
            };
            ev_year == y && ev_month == m
        }
        DateFilter::Today => {
            ev_year == now.year() && ev_month == now.month() && ev_day == now.day()
        }
        DateFilter::Yesterday => {
            ev_year == yesterday.year() && ev_month == yesterday.month() && ev_day == yesterday.day()
        }
        DateFilter::Week => {
            // Get ISO week number AND ISO week year for both event and current date
            // This correctly handles year boundaries (e.g., Dec 30, 2024 is ISO week 1 of 2025)
            let ev_date = NaiveDate::from_ymd_opt(ev_year, ev_month, ev_day).unwrap_or_default();
            let ev_iso_week = ev_date.iso_week();
            let ev_iso_year = ev_iso_week.year();
            let ev_week = ev_iso_week.week();

            let now_date = now.date();
            let now_iso_week = now_date.iso_week();
            let now_iso_year = now_iso_week.year();
            let now_week = now_iso_week.week();

            ev_iso_year == now_iso_year && ev_week == now_week
        }
    }
}

// ISO week number calculation using chrono's built-in support

fn matches_person_filter(event: &Event, person_filter: &Option<String>) -> bool {
    let Some(filter) = person_filter else {
        return true;
    };
    extract_person(&event.summary)
        .map(|p| p.to_lowercase().contains(&filter.to_lowercase()))
        .unwrap_or(false)
}

fn matches_project_filter(event: &Event, project_filter: &Option<String>) -> bool {
    let Some(filter) = project_filter else {
        return true;
    };
    extract_project(&event.summary)
        .map(|p| p.to_lowercase().contains(&filter.to_lowercase()))
        .unwrap_or(false)
}

fn matches_exclude_project_filter(event: &Event, exclude_filters: &[String]) -> bool {
    let Some(project) = extract_project(&event.summary) else {
        return true;
    };
    let project_lower = project.to_lowercase();
    !exclude_filters.iter().any(|f| project_lower.contains(&f.to_lowercase()))
}

fn matches_exclude_filter(event: &Event, exclude_filters: &[String]) -> bool {
    let Some(person) = extract_person(&event.summary) else {
        return true;
    };
    let person_lower = person.to_lowercase();
    !exclude_filters.iter().any(|f| person_lower.contains(&f.to_lowercase()))
}

fn matches_date_range(event: &Event, from: &Option<NaiveDate>, to: &Option<NaiveDate>) -> bool {
    let event_date = event.start.date();
    if let Some(from_date) = from {
        if event_date < *from_date {
            return false;
        }
    }
    if let Some(to_date) = to {
        if event_date > *to_date {
            return false;
        }
    }
    true
}

fn matches_year_filter(event: &Event, year: &Option<i32>) -> bool {
    if let Some(y) = year {
        event.start.year() == *y
    } else {
        true
    }
}

fn matches_month_filter(event: &Event, month: &Option<u32>) -> bool {
    if let Some(m) = month {
        event.start.month() == *m
    } else {
        true
    }
}

// Map day abbreviation to weekday number (Monday = 1)
fn weekday_abbrev_to_num(day: &str) -> Option<u32> {
    match day.to_uppercase().as_str() {
        "MO" => Some(1),
        "TU" => Some(2),
        "WE" => Some(3),
        "TH" => Some(4),
        "FR" => Some(5),
        "SA" => Some(6),
        "SU" => Some(7),
        _ => None,
    }
}

fn matches_weekday_filter(event: &Event, weekdays: &[String]) -> bool {
    if weekdays.is_empty() {
        return true;
    }
    let event_weekday = event.start.weekday().num_days_from_monday() + 1;
    weekdays.iter().any(|day| {
        weekday_abbrev_to_num(day).map(|wd| wd == event_weekday).unwrap_or(false)
    })
}

fn matches_exclude_weekday_filter(event: &Event, exclude_weekdays: &[String]) -> bool {
    if exclude_weekdays.is_empty() {
        return true;
    }
    let event_weekday = event.start.weekday().num_days_from_monday() + 1;
    !exclude_weekdays.iter().any(|day| {
        weekday_abbrev_to_num(day).map(|wd| wd == event_weekday).unwrap_or(false)
    })
}

fn matches_exclude_summary_filter(event: &Event, exclude_filters: &[String]) -> bool {
    if exclude_filters.is_empty() {
        return true;
    }
    let summary_lower = event.summary.to_lowercase();
    !exclude_filters.iter().any(|f| summary_lower.contains(&f.to_lowercase()))
}

fn matches_duration_filter(
    event: &Event,
    min_duration: &Option<Duration>,
    max_duration: &Option<Duration>,
) -> bool {
    let Some(event_mins) = event_duration_minutes(event) else {
        return false; // Skip events with invalid duration
    };

    if let Some(min) = min_duration {
        if event_mins < min.num_minutes() {
            return false;
        }
    }

    if let Some(max) = max_duration {
        if event_mins > max.num_minutes() {
            return false;
        }
    }

    true
}

// JSON serialization structures
#[derive(Serialize)]
struct JsonEvent {
    summary: String,
    person: Option<String>,
    start: String,
    end: String,
    duration_minutes: i64,
    duration_formatted: String,
}

#[derive(Serialize)]
struct PersonHours {
    person: String,
    minutes: i64,
    formatted: String,
}

#[derive(Serialize)]
struct JsonMonthSummary {
    year: i32,
    month: u32,
    month_name: String,
    total_minutes: i64,
    total_formatted: String,
    by_person: Vec<PersonHours>,
    events: Vec<JsonEvent>,
}

#[derive(Serialize)]
struct JsonOutput {
    grand_total_minutes: i64,
    grand_total_formatted: String,
    months: Vec<JsonMonthSummary>,
}

/// Extracts content between matching bracket pairs, or None if invalid/empty.
/// The `open` and `close` args specify which bracket characters to match.
/// Returns the inner content (without brackets) if found and not empty/whitespace.
fn extract_bracketed(summary: &str, open: char, close: char) -> Option<&str> {
    let start = summary.rfind(open)?;
    let end = summary.find(close).filter(|&e| e > start)?;
    let inner = &summary[start + 1..end];
    if !inner.is_empty() && !inner.trim().is_empty() {
        Some(inner)
    } else {
        None
    }
}

/// Extracts person name from event summary using [name] format.
/// Returns the content inside brackets if found and not empty/whitespace.
fn extract_person(summary: &str) -> Option<&str> {
    extract_bracketed(summary, '[', ']')
}

/// Extracts project name from event summary using {project} format.
/// Returns the content inside curly braces if found and not empty/whitespace.
fn extract_project(summary: &str) -> Option<&str> {
    extract_bracketed(summary, '{', '}')
}

fn format_hours(total_minutes: i64) -> String {
    let h = total_minutes / 60;
    let m = total_minutes % 60;
    if m == 0 {
        format!("{}h", h)
    } else {
        format!("{}h {}m", h, m)
    }
}

/// Returns duration in minutes, filtering out non-positive durations
fn event_duration_minutes(event: &Event) -> Option<i64> {
    let mins = (event.end - event.start).num_minutes();
    if mins > 0 { Some(mins) } else { None }
}

/// Escapes a string for CSV output (handles quotes and commas)
fn csv_escape(s: &str) -> String {
    if s.contains('"') || s.contains(',') || s.contains('\n') {
        format!("\"{}\"", s.replace('"', "\"\""))
    } else {
        s.to_string()
    }
}

/// Escapes a string for HTML output
fn html_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;")
}

fn format_percentage(part: i64, total: i64) -> String {
    if total == 0 || part == 0 {
        return "0.0%".to_string();
    }
    let pct = (part as f64 / total as f64) * 100.0;
    format!("{:.1}%", pct)
}

fn main() -> io::Result<()> {
    let args = Args::parse();

    if args.verbose {
        eprintln!("[verbose] Processing {} file(s)", args.files.len());
        if let Some(ref p) = args.person {
            eprintln!("[verbose] Filtering by person: {}", p);
        }
        if let Some(ref p) = args.project {
            eprintln!("[verbose] Filtering by project: {}", p);
        }
        if !args.exclude_person.is_empty() {
            eprintln!("[verbose] Excluding persons: {:?}", args.exclude_person);
        }
        if !args.exclude_project.is_empty() {
            eprintln!("[verbose] Excluding projects: {:?}", args.exclude_project);
        }
        if !args.exclude_summary.is_empty() {
            eprintln!("[verbose] Excluding summaries containing: {:?}", args.exclude_summary);
        }
        if let Some(ref f) = args.from {
            eprintln!("[verbose] From date: {}", f);
        }
        if let Some(ref t) = args.to {
            eprintln!("[verbose] To date: {}", t);
        }
        if let Some(ref y) = args.year {
            eprintln!("[verbose] Filter by year: {}", y);
        }
        if let Some(ref wd) = args.weekdays {
            eprintln!("[verbose] Filter by weekdays: {:?}", wd);
        }
        if let Some(ref wd) = args.exclude_weekdays {
            eprintln!("[verbose] Exclude weekdays: {:?}", wd);
        }
        if let Some(lim) = args.limit {
            eprintln!("[verbose] Limit: {} events", lim);
        }
        if args.today {
            eprintln!("[verbose] Quick filter --today: enabled");
        }
        if args.yesterday {
            eprintln!("[verbose] Quick filter --yesterday: enabled");
        }
    }

    if args.files.is_empty() {
        eprintln!("Error: no .ics files provided");
        std::process::exit(1);
    }

    validate_date_range(&args.from, &args.to)?;
    validate_month(args.month)?;

    // Validate file extensions before processing
    for path in &args.files {
        if let Err(e) = validate_ics_file(path) {
            eprintln!("Error: {}", e);
            std::process::exit(1);
        }
    }

    let mut all_raw_events = Vec::new();
    for path in &args.files {
        if args.verbose {
            eprintln!("[verbose] Reading: {}", path.display());
        }
        let file = File::open(path)
            .map_err(|e| io::Error::new(io::ErrorKind::NotFound, format!("Failed to open {}: {}", path.display(), e)))?;

        let reader = BufReader::new(file);
        let parser = IcalParser::new(reader);

        for calendar in parser {
            match calendar {
                Ok(cal) => {
                    if args.verbose && !cal.events.is_empty() {
                        eprintln!("[verbose] Found {} events in {}", cal.events.len(), path.display());
                    }
                    all_raw_events.extend(extract_raw_events(cal.events));
                }
                Err(_e) if args.quiet => {
                    // Suppress parse errors in quiet mode
                }
                Err(e) => {
                    eprintln!("Warning: failed to parse {}: {}", path.display(), e);
                }
            }
        }
    }

    if args.verbose {
        eprintln!("[verbose] Total raw events: {}", all_raw_events.len());
    }

    let all_events = expand_events(all_raw_events);

    if args.verbose {
        eprintln!("[verbose] Expanded events: {}", all_events.len());
    }

    // Parse duration filters
    let min_duration = args.min_duration.as_ref()
        .and_then(|s| parse_human_duration(s).or_else(|| parse_duration(s)));
    let max_duration = args.max_duration.as_ref()
        .and_then(|s| parse_human_duration(s).or_else(|| parse_duration(s)));

    if args.verbose {
        if let Some(ref d) = min_duration {
            eprintln!("[verbose] Min duration filter: {} minutes", d.num_minutes());
        }
        if let Some(ref d) = max_duration {
            eprintln!("[verbose] Max duration filter: {} minutes", d.num_minutes());
        }
    }

    // Setup output: file or stdout
    let out_writer: Box<dyn Write> = match &args.output {
        Some(path) => {
            let file = File::create(path)?;
            Box::new(file)
        }
        None => Box::new(std::io::stdout()),
    };
    let mut out_writer = out_writer;

    let weekdays_filter = args.weekdays.unwrap_or_default();
    let exclude_weekdays_filter = args.exclude_weekdays.unwrap_or_default();
    
    // Determine effective date filter: explicit flags override --date
    let effective_date = if args.today {
        DateFilter::Today
    } else if args.yesterday {
        DateFilter::Yesterday
    } else {
        args.date.clone()
    };
    
    let filtered: Vec<&Event> = all_events
        .iter()
        .filter(|e| matches_filter(e, &effective_date))
        .filter(|e| matches_person_filter(e, &args.person))
        .filter(|e| matches_project_filter(e, &args.project))
        .filter(|e| matches_exclude_filter(e, &args.exclude_person))
        .filter(|e| matches_exclude_project_filter(e, &args.exclude_project))
        .filter(|e| matches_exclude_summary_filter(e, &args.exclude_summary))
        .filter(|e| matches_date_range(e, &args.from, &args.to))
        .filter(|e| matches_year_filter(e, &args.year))
        .filter(|e| matches_month_filter(e, &args.month))
        .filter(|e| matches_weekday_filter(e, &weekdays_filter))
        .filter(|e| matches_exclude_weekday_filter(e, &exclude_weekdays_filter))
        .filter(|e| matches_duration_filter(e, &min_duration, &max_duration))
        .take(args.limit.unwrap_or(usize::MAX))
        .collect();

    if args.verbose {
        eprintln!("[verbose] Events after filtering: {}", filtered.len());
    }

    // Apply reverse order if requested
    let filtered: Vec<&Event> = if args.reverse {
        let mut rev = filtered;
        rev.reverse();
        rev
    } else {
        filtered
    };

    if filtered.is_empty() {
        println!("No events found for the selected period.");
        return Ok(());
    }

    let grouped: BTreeMap<(i32, u32), MonthSummary> = group_by_month(&filtered);

    if grouped.is_empty() {
        eprintln!("No events found for the selected period.");
        return Ok(());
    }

    // Dry run mode: just show event count
    if args.dry_run {
        let mut by_person: BTreeMap<&str, usize> = BTreeMap::new();
        for event in &filtered {
            let person = extract_person(&event.summary).unwrap_or("(unknown)");
            *by_person.entry(person).or_default() += 1;
        }
        println!("Total events: {}", filtered.len());
        if !by_person.is_empty() {
            println!("\nBy person:");
            for (person, count) in &by_person {
                println!("  {}: {}", person, count);
            }
        }
        return Ok(());
    }

    // Collect all unique persons if --list-persons is requested
    if args.list_persons {
        let mut persons: HashSet<String> = HashSet::new();
        for event in &filtered {
            if let Some(p) = extract_person(&event.summary) {
                persons.insert(p.to_string());
            }
        }
        let mut sorted: Vec<_> = persons.into_iter().collect();
        sorted.sort();
        for person in sorted {
            writeln!(out_writer, "{}", person)?;
        }
        return Ok(());
    }

    // Collect all unique projects if --list-projects is requested
    if args.list_projects {
        let mut projects: HashSet<String> = HashSet::new();
        for event in &filtered {
            if let Some(p) = extract_project(&event.summary) {
                projects.insert(p.to_string());
            }
        }
        let mut sorted: Vec<_> = projects.into_iter().collect();
        sorted.sort();
        for project in sorted {
            writeln!(out_writer, "{}", project)?;
        }
        return Ok(());
    }

    // List all unique events if --list-events is requested
    if args.list_events {
        for event in &filtered {
            writeln!(out_writer, "{} | {} | {}", event.start.format("%Y-%m-%d %H:%M"), event.summary, format_hours(event_duration_minutes(event).unwrap_or(0)))?;
        }
        return Ok(());
    }

    let grand_total_minutes: i64 = filtered.iter()
        .filter_map(|e| event_duration_minutes(e))
        .sum();

    // Show statistics if --stats is requested
    if args.stats {
        let total_events = filtered.len();
        let total_mins = grand_total_minutes;

        // Events per person
        let mut by_person: BTreeMap<&str, i64> = BTreeMap::new();
        for event in &filtered {
            let mins = event_duration_minutes(event).unwrap_or(0);
            let person = extract_person(&event.summary).unwrap_or("(unknown)");
            *by_person.entry(person).or_default() += mins;
        }

        // Events per day of week
        let mut by_weekday: BTreeMap<&str, i64> = BTreeMap::new();
        let weekday_names = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"];
        for event in &filtered {
            let mins = event_duration_minutes(event).unwrap_or(0);
            let wd = event.start.weekday().num_days_from_monday() as usize;
            let name = weekday_names.get(wd).unwrap_or(&"Unknown");
            *by_weekday.entry(name).or_default() += mins;
        }

        // Date range
        let dates: Vec<_> = filtered.iter().map(|e| e.start.date()).collect();
        let min_date = dates.iter().min();
        let max_date = dates.iter().max();

        writeln!(out_writer, "📊 Statistics")?;
        writeln!(out_writer, "{}", colored(color::CYAN, "============"))?;
        writeln!(out_writer)?;
        writeln!(out_writer, "Total events:  {}", colored(color::YELLOW, total_events.to_string()))?;
        writeln!(out_writer, "Total hours:    {}", colored(color::YELLOW, format_hours(total_mins)))?;
        if let (Some(min_d), Some(max_d)) = (min_date, max_date) {
            let days_span = (*max_d - *min_d).num_days() + 1;
            let avg_per_day = if days_span > 0 { total_mins / days_span } else { total_mins };
            writeln!(out_writer, "Date range:     {} to {} ({} days)", min_d, max_d, days_span)?;
            writeln!(out_writer, "Avg per day:    {}", colored(color::YELLOW, format_hours(avg_per_day)))?;
        }

        writeln!(out_writer)?;
        writeln!(out_writer, "{}", colored(color::CYAN, "By Person"))?;
        writeln!(out_writer, "{}", colored(color::CYAN, "--------"))?;
        if !by_person.is_empty() {
            let top_person = by_person.iter().max_by_key(|(_, v)| *v);
            for (person, mins) in &by_person {
                let marker = if Some((person, mins)) == top_person { " 🏆" } else { "" };
                let pct = format_percentage(*mins, total_mins);
                writeln!(out_writer, "  {}  {:>6}  ({}){}", colored(color::YELLOW, format_hours(*mins)), colored(color::MAGENTA, pct), person, marker)?;
            }
        } else {
            writeln!(out_writer, "  (no person data)")?;
        }

        writeln!(out_writer)?;
        writeln!(out_writer, "{}", colored(color::CYAN, "By Weekday"))?;
        writeln!(out_writer, "{}", colored(color::CYAN, "------------"))?;
        for (day, mins) in &by_weekday {
            writeln!(out_writer, "  {}  {}", colored(color::YELLOW, format_hours(*mins)), day)?;
        }

        return Ok(());
    }

    match args.format {
        OutputFormat::Csv => {
            let mut wtr = csv::Writer::from_writer(out_writer);
            
            // In quiet/sum_only mode, output only totals per person
            if args.quiet || args.sum_only {
                // Build per-person summary
                let all_by_person: BTreeMap<&str, i64> = filtered
                    .iter()
                    .filter_map(|e| {
                        let mins = event_duration_minutes(e)?;
                        Some((extract_person(&e.summary).unwrap_or("(unknown)"), mins))
                    })
                    .fold(BTreeMap::new(), |mut acc, (person, mins)| {
                        *acc.entry(person).or_default() += mins;
                        acc
                    });
                
                wtr.write_record(["person", "total_minutes", "total_formatted", "percentage"])
                    .ok();
                for (person, mins) in &all_by_person {
                    wtr.write_record(&[
                        csv_escape(person),
                        mins.to_string(),
                        format_hours(*mins),
                        format_percentage(*mins, grand_total_minutes),
                    ])
                    .ok();
                }
                wtr.write_record(&[
                    "TOTAL".to_string(),
                    grand_total_minutes.to_string(),
                    format_hours(grand_total_minutes),
                    "100.0%".to_string(),
                ])
                .ok();
            } else {
                // Full output with individual events
                wtr.write_record(["date", "start", "end", "duration_minutes", "person", "summary"])
                    .ok();
                for event in &filtered {
                    let mins = match event_duration_minutes(event) {
                        Some(m) => m,
                        None => continue,
                    };
                    let person = extract_person(&event.summary).unwrap_or("(unknown)");
                    wtr.write_record(&[
                        event.start.format("%Y-%m-%d").to_string(),
                        event.start.format("%H:%M").to_string(),
                        event.end.format("%H:%M").to_string(),
                        mins.to_string(),
                        csv_escape(person),
                        csv_escape(&event.summary),
                    ])
                    .ok();
                }
                // Add grand total as final row
                wtr.write_record(&[
                    "TOTAL".to_string(),
                    "".to_string(),
                    "".to_string(),
                    grand_total_minutes.to_string(),
                    "".to_string(),
                    "".to_string(),
                ])
                .ok();
            }
            wtr.flush().ok();
        }
        OutputFormat::Json => {
            let mut months_json: Vec<JsonMonthSummary> = Vec::new();
            for ((year, month), summary) in &grouped {
                let mut month_minutes: i64 = 0;
                let mut month_by_person: BTreeMap<String, i64> = BTreeMap::new();
                let mut events_json: Vec<JsonEvent> = Vec::new();
                
                for event in &summary.events {
                    if let Some(mins) = event_duration_minutes(event) {
                        month_minutes += mins;
                        let person = extract_person(&event.summary).map(|s| s.to_string());
                        if let Some(ref p) = person {
                            *month_by_person.entry(p.clone()).or_default() += mins;
                        }
                        events_json.push(JsonEvent {
                            summary: event.summary.clone(),
                            person,
                            start: event.start.format("%Y-%m-%d %H:%M").to_string(),
                            end: event.end.format("%Y-%m-%d %H:%M").to_string(),
                            duration_minutes: mins,
                            duration_formatted: format_hours(mins),
                        });
                    }
                }
                
                let by_person: Vec<PersonHours> = month_by_person
                    .into_iter()
                    .map(|(p, m)| PersonHours {
                        person: p,
                        minutes: m,
                        formatted: format_hours(m),
                    })
                    .collect();
                
                months_json.push(JsonMonthSummary {
                    year: *year,
                    month: *month,
                    month_name: summary.month_name.clone(),
                    total_minutes: month_minutes,
                    total_formatted: format_hours(month_minutes),
                    by_person,
                    events: events_json,
                });
            }
            
            let json_output = JsonOutput {
                grand_total_minutes,
                grand_total_formatted: format_hours(grand_total_minutes),
                months: months_json,
            };
            let json_str = if args.compact {
                serde_json::to_string(&json_output).unwrap_or_else(|_| "{}".to_string())
            } else {
                serde_json::to_string_pretty(&json_output).unwrap_or_else(|_| "{}".to_string())
            };
            writeln!(out_writer, "{}", json_str)?;
        }
        OutputFormat::Ical => {
            writeln!(out_writer, "BEGIN:VCALENDAR")?;
            writeln!(out_writer, "VERSION:2.0")?;
            writeln!(out_writer, "PRODID:-//proton-extractor//EN")?;
            for event in &filtered {
                writeln!(out_writer, "BEGIN:VEVENT")?;
                writeln!(out_writer, "UID:{}@proton-extractor", event.start.and_utc().timestamp())?;
                writeln!(out_writer, "DTSTAMP:{}", event.start.format("%Y%m%dT%H%M%S"))?;
                writeln!(out_writer, "DTSTART:{}", event.start.format("%Y%m%dT%H%M%S"))?;
                writeln!(out_writer, "DTEND:{}", event.end.format("%Y%m%dT%H%M%S"))?;
                // Escape summary for iCal format
                let summary_escaped = event.summary
                    .replace("\\", "\\\\")
                    .replace(";", "\\;")
                    .replace(",", "\\,")
                    .replace("\n", "\\n");
                writeln!(out_writer, "SUMMARY:{}", summary_escaped)?;
                writeln!(out_writer, "END:VEVENT")?;
            }
            writeln!(out_writer, "END:VCALENDAR")?;
        }
        OutputFormat::Html => {
            // Build per-person summary
            let all_by_person: BTreeMap<&str, i64> = filtered
                .iter()
                .filter_map(|e| {
                    let mins = event_duration_minutes(e)?;
                    Some((extract_person(&e.summary).unwrap_or("(unknown)"), mins))
                })
                .fold(BTreeMap::new(), |mut acc: BTreeMap<&str, i64>, (person, mins)| {
                    *acc.entry(person).or_default() += mins;
                    acc
                });

            writeln!(out_writer, "<!DOCTYPE html>")?;
            writeln!(out_writer, "<html lang=\"en\">")?;
            writeln!(out_writer, "<head>")?;
            writeln!(out_writer, "  <meta charset=\"UTF-8\">")?;
            writeln!(out_writer, "  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\">")?;
            writeln!(out_writer, "  <title>Time Report - proton-extractor</title>")?;
            writeln!(out_writer, "  <style>")?;
            writeln!(out_writer, "    body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; max-width: 800px; margin: 0 auto; padding: 20px; background: #f5f5f5; }}")?;
            writeln!(out_writer, "    h1 {{ color: #333; border-bottom: 2px solid #4CAF50; padding-bottom: 10px; }}")?;
            writeln!(out_writer, "    h2 {{ color: #555; margin-top: 30px; }}")?;
            writeln!(out_writer, "    .month-section {{ background: white; border-radius: 8px; padding: 20px; margin: 20px 0; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }}")?;
            writeln!(out_writer, "    .event-list {{ list-style: none; padding: 0; }}")?;
            writeln!(out_writer, "    .event-item {{ padding: 8px 0; border-bottom: 1px solid #eee; display: flex; justify-content: space-between; }}")?;
            writeln!(out_writer, "    .event-item:last-child {{ border-bottom: none; }}")?;
            writeln!(out_writer, "    .duration {{ font-weight: bold; color: #4CAF50; }}")?;
            writeln!(out_writer, "    .summary {{ color: #333; }}")?;
            writeln!(out_writer, "    .person-summary {{ display: flex; justify-content: space-between; padding: 8px 0; border-bottom: 1px solid #eee; }}")?;
            writeln!(out_writer, "    .total {{ font-weight: bold; font-size: 1.2em; color: #333; padding: 15px 0; border-top: 2px solid #4CAF50; margin-top: 10px; }}")?;
            writeln!(out_writer, "    .grand-total {{ background: #4CAF50; color: white; padding: 20px; border-radius: 8px; margin: 20px 0; text-align: center; font-size: 1.5em; }}")?;
            writeln!(out_writer, "    .person-breakdown {{ background: #f9f9f9; padding: 15px; border-radius: 4px; margin-top: 15px; }}")?;
            writeln!(out_writer, "    .percentage {{ color: #888; font-size: 0.9em; }}")?;
            writeln!(out_writer, "  </style>")?;
            writeln!(out_writer, "</head>")?;
            writeln!(out_writer, "<body>")?;
            writeln!(out_writer, "  <h1>⏱️ Time Report</h1>")?;
            writeln!(out_writer, "  <p>Generated by <a href=\"https://github.com/JeremySomsouk/proton-extractor\">proton-extractor</a></p>")?;

            for ((_year, _month), summary) in &grouped {
                summary.write_html(&mut out_writer, args.quiet, args.sum_only, grand_total_minutes)?;
            }

            if grand_total_minutes > 0 && !all_by_person.is_empty() && !args.sum_only {
                writeln!(out_writer, "  <div class=\"grand-total\">")?;
                writeln!(out_writer, "    🎯 Grand Total: {}", format_hours(grand_total_minutes))?;
                writeln!(out_writer, "  </div>")?;
                writeln!(out_writer, "  <div class=\"month-section\">")?;
                writeln!(out_writer, "    <h2>👥 All Persons Summary</h2>")?;
                writeln!(out_writer, "    <div class=\"person-breakdown\">")?;
                for (person, mins) in &all_by_person {
                    let pct = format_percentage(*mins, grand_total_minutes);
                    writeln!(out_writer, "    <div class=\"person-summary\">")?;
                    writeln!(out_writer, "      <span>{}</span>", html_escape(person))?;
                    writeln!(out_writer, "      <span><strong>{}</strong> <span class=\"percentage\">({})</span></span>", format_hours(*mins), pct)?;
                    writeln!(out_writer, "    </div>")?;
                }
                writeln!(out_writer, "    </div>")?;
                writeln!(out_writer, "  </div>")?;
            }

            writeln!(out_writer, "</body>")?;
            writeln!(out_writer, "</html>")?;
        }
        OutputFormat::Text => {
            // Build per-person summary across all events
            let all_by_person: BTreeMap<&str, i64> = filtered
                .iter()
                .filter_map(|e| {
                    let mins = event_duration_minutes(e)?;
                    Some((extract_person(&e.summary).unwrap_or("(unknown)"), mins))
                })
                .fold(BTreeMap::new(), |mut acc: BTreeMap<&str, i64>, (person, mins)| {
                    *acc.entry(person).or_default() += mins;
                    acc
                });

            for ((year, _month), summary) in &grouped {
                writeln!(out_writer)?;
                writeln!(out_writer, "{}", colored(color::CYAN, format!("--- {} {} ---", summary.month_name, year)))?;

                let month_by_person = summary.by_person();

                if !args.quiet && !args.sum_only {
                    for event in &summary.events {
                        if let Some(mins) = event_duration_minutes(event) {
                            writeln!(out_writer, "  {}  {}", colored(color::YELLOW, format_hours(mins)), event.summary)?;
                        }
                    }
                }

                writeln!(out_writer, "  {}", colored(color::MAGENTA, "------"))?;
                for (person, mins) in &month_by_person {
                    writeln!(out_writer, "  {}  {}", colored(color::YELLOW, format_hours(*mins)), person)?;
                }
                writeln!(out_writer, "  {}  {}", colored(color::GREEN, format_hours(summary.total_minutes())), colored(color::BOLD, "TOTAL"))?;
            }

            if grand_total_minutes > 0 && grouped.len() > 1 {
                writeln!(out_writer)?;
                writeln!(out_writer, "{}", colored(color::GREEN, format!("=== Grand Total: {} ===", format_hours(grand_total_minutes))))?;
            }

            if !all_by_person.is_empty() && !args.sum_only {
                writeln!(out_writer)?;
                writeln!(out_writer, "{}", colored(color::CYAN, "=== Hours per person ==="))?;
                for (person, mins) in &all_by_person {
                    writeln!(out_writer, "  {}  {:>6}  {}", colored(color::YELLOW, format_hours(*mins)), colored(color::MAGENTA, format_percentage(*mins, grand_total_minutes)), person)?;
                }
            }
        }
        OutputFormat::Markdown => {
            // Build per-person summary across all events
            let all_by_person: BTreeMap<&str, i64> = filtered
                .iter()
                .filter_map(|e| {
                    let mins = event_duration_minutes(e)?;
                    Some((extract_person(&e.summary).unwrap_or("(unknown)"), mins))
                })
                .fold(BTreeMap::new(), |mut acc: BTreeMap<&str, i64>, (person, mins)| {
                    *acc.entry(person).or_default() += mins;
                    acc
                });

            for ((year, _month), summary) in &grouped {
                writeln!(out_writer)?;
                writeln!(out_writer, "## {} {}", summary.month_name, year)?;
                writeln!(out_writer)?;

                let month_by_person = summary.by_person();

                if !args.quiet && !args.sum_only {
                    writeln!(out_writer, "| Duration | Event |")?;
                    writeln!(out_writer, "|----------|-------|")?;
                    for event in &summary.events {
                        if let Some(mins) = event_duration_minutes(event) {
                            writeln!(out_writer, "| {} | {} |", format_hours(mins), event.summary)?;
                        }
                    }
                    writeln!(out_writer)?;
                }

                writeln!(out_writer, "### 👤 By Person")?;
                writeln!(out_writer)?;
                writeln!(out_writer, "| Person | Hours | % |")?;
                writeln!(out_writer, "|--------|-------|---|")?;
                for (person, mins) in &month_by_person {
                    writeln!(out_writer, "| {} | {} | {} |", person, format_hours(*mins), format_percentage(*mins, summary.total_minutes()))?;
                }
                writeln!(out_writer, "| **TOTAL** | **{}** | 100% |", format_hours(summary.total_minutes()))?;
                writeln!(out_writer)?;
            }

            if grand_total_minutes > 0 && !all_by_person.is_empty() {
                writeln!(out_writer, "## 🎯 Grand Total: {}", format_hours(grand_total_minutes))?;
                writeln!(out_writer)?;
                writeln!(out_writer, "### 👥 Hours per Person")?;
                writeln!(out_writer)?;
                writeln!(out_writer, "| Person | Hours | % |")?;
                writeln!(out_writer, "|--------|-------|---|")?;
                for (person, mins) in &all_by_person {
                    writeln!(out_writer, "| {} | {} | {} |", person, format_hours(*mins), format_percentage(*mins, grand_total_minutes))?;
                }
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Timelike;

    #[test]
    fn test_extract_person_valid() {
        assert_eq!(extract_person("Meeting with [John Doe]"), Some("John Doe"));
        assert_eq!(extract_person("[Alice] standup"), Some("Alice"));
        assert_eq!(extract_person("  [  Bob  ]  report"), Some("  Bob  "));
    }

    #[test]
    fn test_extract_person_no_brackets() {
        assert_eq!(extract_person("Regular meeting"), None);
        assert_eq!(extract_person("[Only opening"), None);
        assert_eq!(extract_person("Only closing]"), None);
    }

    #[test]
    fn test_extract_person_empty() {
        assert_eq!(extract_person("[]"), None);
        assert_eq!(extract_person("[ ]"), None);
    }

    #[test]
    fn test_extract_project_valid() {
        assert_eq!(extract_project("Meeting {Alpha} discussion"), Some("Alpha"));
        assert_eq!(extract_project("{Beta} standup"), Some("Beta"));
        assert_eq!(extract_project("  {  Gamma  }  report"), Some("  Gamma  "));
    }

    #[test]
    fn test_extract_project_no_braces() {
        assert_eq!(extract_project("Regular meeting"), None);
        assert_eq!(extract_project("{Only opening"), None);
        assert_eq!(extract_project("Only closing}"), None);
    }

    #[test]
    fn test_extract_project_empty() {
        assert_eq!(extract_project("{}"), None);
        assert_eq!(extract_project("{ }"), None);
    }

    #[test]
    fn test_format_hours_whole() {
        assert_eq!(format_hours(60), "1h");
        assert_eq!(format_hours(120), "2h");
        assert_eq!(format_hours(480), "8h");
    }

    #[test]
    fn test_format_hours_with_minutes() {
        assert_eq!(format_hours(90), "1h 30m");
        assert_eq!(format_hours(45), "0h 45m");
        assert_eq!(format_hours(150), "2h 30m");
    }

    #[test]
    fn test_format_percentage() {
        assert_eq!(format_percentage(30, 100), "30.0%");
        assert_eq!(format_percentage(25, 100), "25.0%");
        assert_eq!(format_percentage(50, 100), "50.0%");
        assert_eq!(format_percentage(10, 1000), "1.0%");
        assert_eq!(format_percentage(333, 1000), "33.3%");
        assert_eq!(format_percentage(0, 100), "0.0%");
        assert_eq!(format_percentage(100, 0), "0.0%"); // avoid division by zero
    }

    #[test]
    fn test_parse_ical_datetime() {
        let dt = parse_ical_datetime("20240315T090000");
        assert!(dt.is_some());
        let dt = dt.unwrap();
        assert_eq!(dt.year(), 2024);
        assert_eq!(dt.month(), 3);
        assert_eq!(dt.day(), 15);

        // Z suffix should be handled
        assert!(parse_ical_datetime("20240315T090000Z").is_some());

        // Date-only format
        let dt = parse_ical_datetime("20240315");
        assert!(dt.is_some());
        assert_eq!(dt.unwrap().hour(), 0);
    }

    #[test]
    fn test_parse_ical_datetime_with_utc_offset() {
        // UTC+0530 offset: 9:00 local = 3:30 UTC
        let dt = parse_ical_datetime("20240315T090000+0530");
        assert!(dt.is_some());
        let dt = dt.unwrap();
        assert_eq!(dt.hour(), 3);
        assert_eq!(dt.minute(), 30);

        // UTC-0800 offset: 9:00 local = 17:00 UTC
        let dt = parse_ical_datetime("20240315T090000-0800");
        assert!(dt.is_some());
        let dt = dt.unwrap();
        assert_eq!(dt.hour(), 17);
        assert_eq!(dt.minute(), 0);

        // UTC+00:00 offset format with colon
        let dt = parse_ical_datetime("20240315T090000+0000");
        assert!(dt.is_some());
        let dt = dt.unwrap();
        assert_eq!(dt.hour(), 9);
        assert_eq!(dt.minute(), 0);

        // Negative offset with colon: 10:00-0500 = 15:00 UTC
        let dt = parse_ical_datetime("20240315T100000-0500");
        assert!(dt.is_some());
        let dt = dt.unwrap();
        assert_eq!(dt.hour(), 15);
        assert_eq!(dt.minute(), 0);
    }

    #[test]
    fn test_parse_rrule() {
        let result = parse_rrule("FREQ=WEEKLY;UNTIL=20240315T090000Z");
        assert!(result.is_some());
        let (freq, until, byday, interval, count) = result.unwrap();
        assert_eq!(freq, "WEEKLY");
        assert_eq!(until.format("%Y%m%d").to_string(), "20240315"); // Date only, time is 00:00:00
        assert!(byday.is_none());
        assert_eq!(interval, None);
        assert_eq!(count, None);
        
        // BYDAY extraction
        let result = parse_rrule("FREQ=WEEKLY;BYDAY=MO,WE,FR;UNTIL=20240315T090000Z");
        assert!(result.is_some());
        let (freq, _, byday, _, _) = result.unwrap();
        assert_eq!(freq, "WEEKLY");
        assert_eq!(byday, Some(vec!["MO".to_string(), "WE".to_string(), "FR".to_string()]));
        
        // Missing UNTIL gets a default
        let result = parse_rrule("FREQ=DAILY");
        assert!(result.is_some());
        let (_, until, _, _, _) = result.unwrap();
        assert_eq!(until.format("%Y").to_string(), "2099"); // Should have default date
        
        // INTERVAL extraction
        let result = parse_rrule("FREQ=WEEKLY;INTERVAL=2;UNTIL=20240315T090000Z");
        assert!(result.is_some());
        let (freq, _, _, interval, _) = result.unwrap();
        assert_eq!(freq, "WEEKLY");
        assert_eq!(interval, Some(2));
        
        // COUNT extraction
        let result = parse_rrule("FREQ=DAILY;COUNT=10");
        assert!(result.is_some());
        let (_, _, _, _, count) = result.unwrap();
        assert_eq!(count, Some(10));
    }

    #[test]
    fn test_parse_duration() {
        // Days
        assert_eq!(parse_duration("P1D"), Some(Duration::days(1)));
        assert_eq!(parse_duration("P7D"), Some(Duration::days(7)));
        
        // Weeks
        assert_eq!(parse_duration("P1W"), Some(Duration::weeks(1)));
        assert_eq!(parse_duration("P2W"), Some(Duration::weeks(2)));
        
        // Hours and minutes
        assert_eq!(parse_duration("PT1H"), Some(Duration::hours(1)));
        assert_eq!(parse_duration("PT30M"), Some(Duration::minutes(30)));
        assert_eq!(parse_duration("PT1H30M"), Some(Duration::hours(1) + Duration::minutes(30)));
        
        // Combined
        assert_eq!(parse_duration("P1DT1H"), Some(Duration::days(1) + Duration::hours(1)));
        assert_eq!(parse_duration("P1DT1H30M"), Some(Duration::days(1) + Duration::hours(1) + Duration::minutes(30)));
        
        // Invalid
        assert_eq!(parse_duration(""), None);
        assert_eq!(parse_duration("INVALID"), None);
        assert_eq!(parse_duration("P"), None);
        assert_eq!(parse_duration("123"), None); // bare number
    }

    #[test]
    fn test_matches_person_filter() {
        let event = Event::new(
            "Meeting with [John Doe]".to_string(),
            NaiveDate::from_ymd_opt(2024, 3, 15).unwrap().and_hms_opt(9, 0, 0).unwrap(),
            NaiveDate::from_ymd_opt(2024, 3, 15).unwrap().and_hms_opt(10, 0, 0).unwrap(),
        );
        
        assert!(matches_person_filter(&event, &None));
        assert!(matches_person_filter(&event, &Some("John".to_string())));
        assert!(matches_person_filter(&event, &Some("john".to_string())));
        assert!(!matches_person_filter(&event, &Some("Jane".to_string())));
    }

    #[test]
    fn test_matches_project_filter() {
        let event = Event::new(
            "Meeting {Project Alpha}".to_string(),
            NaiveDate::from_ymd_opt(2024, 3, 15).unwrap().and_hms_opt(9, 0, 0).unwrap(),
            NaiveDate::from_ymd_opt(2024, 3, 15).unwrap().and_hms_opt(10, 0, 0).unwrap(),
        );
        
        assert!(matches_project_filter(&event, &None));
        assert!(matches_project_filter(&event, &Some("Alpha".to_string())));
        assert!(matches_project_filter(&event, &Some("alpha".to_string())));
        assert!(matches_project_filter(&event, &Some("Project".to_string())));
        assert!(!matches_project_filter(&event, &Some("Beta".to_string())));
        
        // Event without project
        let no_project = Event::new(
            "Regular meeting".to_string(),
            NaiveDate::from_ymd_opt(2024, 3, 15).unwrap().and_hms_opt(9, 0, 0).unwrap(),
            NaiveDate::from_ymd_opt(2024, 3, 15).unwrap().and_hms_opt(10, 0, 0).unwrap(),
        );
        assert!(!matches_project_filter(&no_project, &Some("anything".to_string())));
    }

    #[test]
    fn test_matches_date_range() {
        let event = Event::new(
            "Test [Event]".to_string(),
            NaiveDate::from_ymd_opt(2024, 3, 15).unwrap().and_hms_opt(9, 0, 0).unwrap(),
            NaiveDate::from_ymd_opt(2024, 3, 15).unwrap().and_hms_opt(10, 0, 0).unwrap(),
        );
        
        assert!(matches_date_range(&event, &None, &None));
        assert!(matches_date_range(&event, &Some(NaiveDate::from_ymd_opt(2024, 3, 1).unwrap()), &None));
        assert!(matches_date_range(&event, &None, &Some(NaiveDate::from_ymd_opt(2024, 3, 31).unwrap())));
        assert!(!matches_date_range(&event, &Some(NaiveDate::from_ymd_opt(2024, 4, 1).unwrap()), &None));
        assert!(!matches_date_range(&event, &None, &Some(NaiveDate::from_ymd_opt(2024, 3, 1).unwrap())));
    }

    #[test]
    fn test_matches_filter_today() {
        let today = Local::now().naive_local().date();
        let today_event = Event::new(
            "Today meeting [Alice]".to_string(),
            today.and_hms_opt(9, 0, 0).unwrap(),
            today.and_hms_opt(10, 0, 0).unwrap(),
        );
        let yesterday = today - chrono::Duration::days(1);
        let yesterday_event = Event::new(
            "Yesterday meeting [Bob]".to_string(),
            yesterday.and_hms_opt(9, 0, 0).unwrap(),
            yesterday.and_hms_opt(10, 0, 0).unwrap(),
        );
        
        assert!(matches_filter(&today_event, &DateFilter::Today));
        assert!(!matches_filter(&yesterday_event, &DateFilter::Today));
        assert!(matches_filter(&today_event, &DateFilter::All));
        assert!(matches_filter(&yesterday_event, &DateFilter::All));
    }

    #[test]
    fn test_matches_filter_yesterday() {
        let today = Local::now().naive_local().date();
        let yesterday = today - chrono::Duration::days(1);
        let today_event = Event::new(
            "Today meeting [Alice]".to_string(),
            today.and_hms_opt(9, 0, 0).unwrap(),
            today.and_hms_opt(10, 0, 0).unwrap(),
        );
        let yesterday_event = Event::new(
            "Yesterday meeting [Bob]".to_string(),
            yesterday.and_hms_opt(9, 0, 0).unwrap(),
            yesterday.and_hms_opt(10, 0, 0).unwrap(),
        );
        let two_days_ago = today - chrono::Duration::days(2);
        let two_days_ago_event = Event::new(
            "Two days ago [Carol]".to_string(),
            two_days_ago.and_hms_opt(9, 0, 0).unwrap(),
            two_days_ago.and_hms_opt(10, 0, 0).unwrap(),
        );
        
        assert!(matches_filter(&yesterday_event, &DateFilter::Yesterday));
        assert!(!matches_filter(&today_event, &DateFilter::Yesterday));
        assert!(!matches_filter(&two_days_ago_event, &DateFilter::Yesterday));
    }

    #[test]
    fn test_matches_filter_week() {
        let today = Local::now().naive_local().date();
        let today_event = Event::new(
            "Today meeting [Alice]".to_string(),
            today.and_hms_opt(9, 0, 0).unwrap(),
            today.and_hms_opt(10, 0, 0).unwrap(),
        );
        let last_week = today - chrono::Duration::days(7);
        let last_week_event = Event::new(
            "Last week meeting [Bob]".to_string(),
            last_week.and_hms_opt(9, 0, 0).unwrap(),
            last_week.and_hms_opt(10, 0, 0).unwrap(),
        );
        
        assert!(matches_filter(&today_event, &DateFilter::Week));
        assert!(!matches_filter(&last_week_event, &DateFilter::Week));
    }

    #[test]
    fn test_validate_date_range_valid() {
        let from = NaiveDate::from_ymd_opt(2024, 3, 1).unwrap();
        let to = NaiveDate::from_ymd_opt(2024, 3, 31).unwrap();
        assert!(validate_date_range(&Some(from), &Some(to)).is_ok());
        
        // Same date is valid
        let same = NaiveDate::from_ymd_opt(2024, 3, 15).unwrap();
        assert!(validate_date_range(&Some(same), &Some(same)).is_ok());
        
        // None values are valid
        assert!(validate_date_range(&None, &None).is_ok());
        assert!(validate_date_range(&Some(from), &None).is_ok());
        assert!(validate_date_range(&None, &Some(to)).is_ok());
    }

    #[test]
    fn test_validate_date_range_invalid() {
        let from = NaiveDate::from_ymd_opt(2024, 4, 1).unwrap();
        let to = NaiveDate::from_ymd_opt(2024, 3, 1).unwrap();
        let result = validate_date_range(&Some(from), &Some(to));
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_ics_file() {
        // Valid .ics file
        let valid = PathBuf::from("calendar.ics");
        assert!(validate_ics_file(&valid).is_ok());

        let valid_upper = PathBuf::from("CALENDAR.ICS");
        assert!(validate_ics_file(&valid_upper).is_ok());

        // Invalid extensions
        let txt = PathBuf::from("data.txt");
        let err = validate_ics_file(&txt).unwrap_err();
        assert!(err.to_string().contains("invalid extension"));

        let json = PathBuf::from("data.json");
        let err = validate_ics_file(&json).unwrap_err();
        assert!(err.to_string().contains(".json"));

        // No extension
        let no_ext = PathBuf::from("noextension");
        let err = validate_ics_file(&no_ext).unwrap_err();
        assert!(err.to_string().contains("no file extension"));
    }

    #[test]
    fn test_csv_escape() {
        assert_eq!(csv_escape("simple"), "simple");
        assert_eq!(csv_escape("has,comma"), "\"has,comma\"");
        assert_eq!(csv_escape("has\"quote"), "\"has\"\"quote\"");
        assert_eq!(csv_escape("has\nnewline"), "\"has\nnewline\"");
        assert_eq!(csv_escape("has,comma\"and\nnewline"), "\"has,comma\"\"and\nnewline\"");
    }

    #[test]
    fn test_html_escape() {
        assert_eq!(html_escape("simple"), "simple");
        assert_eq!(html_escape("has & ampersand"), "has &amp; ampersand");
        assert_eq!(html_escape("has < less"), "has &lt; less");
        assert_eq!(html_escape("has > greater"), "has &gt; greater");
        assert_eq!(html_escape("has \" quotes"), "has &quot; quotes");
        assert_eq!(html_escape("has ' apostrophe"), "has &#39; apostrophe");
        assert_eq!(html_escape("&<>\"'"), "&amp;&lt;&gt;&quot;&#39;");
    }

    #[test]
    fn test_event_duration_minutes() {
        let event = Event::new(
            "Test".to_string(),
            NaiveDate::from_ymd_opt(2024, 3, 15).unwrap().and_hms_opt(9, 0, 0).unwrap(),
            NaiveDate::from_ymd_opt(2024, 3, 15).unwrap().and_hms_opt(10, 0, 0).unwrap(),
        );
        assert_eq!(event_duration_minutes(&event), Some(60));

        // Zero duration
        let zero = Event::new(
            "Zero".to_string(),
            NaiveDate::from_ymd_opt(2024, 3, 15).unwrap().and_hms_opt(9, 0, 0).unwrap(),
            NaiveDate::from_ymd_opt(2024, 3, 15).unwrap().and_hms_opt(9, 0, 0).unwrap(),
        );
        assert_eq!(event_duration_minutes(&zero), None);

        // Negative duration
        let neg = Event::new(
            "Neg".to_string(),
            NaiveDate::from_ymd_opt(2024, 3, 15).unwrap().and_hms_opt(10, 0, 0).unwrap(),
            NaiveDate::from_ymd_opt(2024, 3, 15).unwrap().and_hms_opt(9, 0, 0).unwrap(),
        );
        assert_eq!(event_duration_minutes(&neg), None);
    }

    #[test]
    fn test_expand_events_simple() {
        let raw = RawEvent {
            summary: "Meeting [Alice]".to_string(),
            start: NaiveDate::from_ymd_opt(2024, 3, 1).unwrap().and_hms_opt(9, 0, 0).unwrap(),
            end: NaiveDate::from_ymd_opt(2024, 3, 1).unwrap().and_hms_opt(10, 0, 0).unwrap(),
            uid: "uid1".to_string(),
            rrule: None,
            exdates: vec![],
            recurrence_id: None,
        };
        let expanded = expand_events(vec![raw]);
        assert_eq!(expanded.len(), 1);
        assert_eq!(expanded[0].summary, "Meeting [Alice]");
    }

    #[test]
    fn test_expand_events_filters_zero_duration() {
        let zero_duration = RawEvent {
            summary: "Zero [Bob]".to_string(),
            start: NaiveDate::from_ymd_opt(2024, 3, 1).unwrap().and_hms_opt(9, 0, 0).unwrap(),
            end: NaiveDate::from_ymd_opt(2024, 3, 1).unwrap().and_hms_opt(9, 0, 0).unwrap(),
            uid: "uid1".to_string(),
            rrule: None,
            exdates: vec![],
            recurrence_id: None,
        };
        let expanded = expand_events(vec![zero_duration]);
        assert!(expanded.is_empty());
    }

    #[test]
    fn test_expand_events_daily_recurrence() {
        let daily = RawEvent {
            summary: "Daily [Carol]".to_string(),
            start: NaiveDate::from_ymd_opt(2024, 3, 1).unwrap().and_hms_opt(9, 0, 0).unwrap(),
            end: NaiveDate::from_ymd_opt(2024, 3, 1).unwrap().and_hms_opt(10, 0, 0).unwrap(),
            uid: "uid1".to_string(),
            rrule: Some("FREQ=DAILY;UNTIL=20240305T235959".to_string()),
            exdates: vec![],
            recurrence_id: None,
        };
        let expanded = expand_events(vec![daily]);
        // 5 days: March 1, 2, 3, 4, 5
        assert_eq!(expanded.len(), 5);
        assert_eq!(expanded[0].start.date(), NaiveDate::from_ymd_opt(2024, 3, 1).unwrap());
        assert_eq!(expanded[4].start.date(), NaiveDate::from_ymd_opt(2024, 3, 5).unwrap());
    }

    #[test]
    fn test_expand_events_with_exdates() {
        let with_exdate = RawEvent {
            summary: "Weekly [Dave]".to_string(),
            start: NaiveDate::from_ymd_opt(2024, 3, 1).unwrap().and_hms_opt(9, 0, 0).unwrap(),
            end: NaiveDate::from_ymd_opt(2024, 3, 1).unwrap().and_hms_opt(10, 0, 0).unwrap(),
            uid: "uid1".to_string(),
            rrule: Some("FREQ=WEEKLY;UNTIL=20240315T235959".to_string()),
            exdates: vec![NaiveDate::from_ymd_opt(2024, 3, 8).unwrap()],
            recurrence_id: None,
        };
        let expanded = expand_events(vec![with_exdate]);
        // 3 weeks, minus 1 exdate = 2 events (March 1, 15)
        assert_eq!(expanded.len(), 2);
    }

    #[test]
    fn test_expand_events_with_interval() {
        // Every 2 weeks: March 1, March 15, March 29
        let biweekly = RawEvent {
            summary: "Biweekly [Carol]".to_string(),
            start: NaiveDate::from_ymd_opt(2024, 3, 1).unwrap().and_hms_opt(9, 0, 0).unwrap(),
            end: NaiveDate::from_ymd_opt(2024, 3, 1).unwrap().and_hms_opt(10, 0, 0).unwrap(),
            uid: "uid1".to_string(),
            rrule: Some("FREQ=WEEKLY;INTERVAL=2;UNTIL=20240331T235959".to_string()),
            exdates: vec![],
            recurrence_id: None,
        };
        let expanded = expand_events(vec![biweekly]);
        assert_eq!(expanded.len(), 3);
        assert_eq!(expanded[0].start.date(), NaiveDate::from_ymd_opt(2024, 3, 1).unwrap());
        assert_eq!(expanded[1].start.date(), NaiveDate::from_ymd_opt(2024, 3, 15).unwrap());
        assert_eq!(expanded[2].start.date(), NaiveDate::from_ymd_opt(2024, 3, 29).unwrap());
    }

    #[test]
    fn test_expand_events_with_count() {
        // Daily for 5 occurrences
        let daily_count = RawEvent {
            summary: "Daily 5 times [Eve]".to_string(),
            start: NaiveDate::from_ymd_opt(2024, 3, 1).unwrap().and_hms_opt(9, 0, 0).unwrap(),
            end: NaiveDate::from_ymd_opt(2024, 3, 1).unwrap().and_hms_opt(10, 0, 0).unwrap(),
            uid: "uid1".to_string(),
            rrule: Some("FREQ=DAILY;COUNT=5".to_string()),
            exdates: vec![],
            recurrence_id: None,
        };
        let expanded = expand_events(vec![daily_count]);
        // Should only produce 5 events despite no UNTIL
        assert_eq!(expanded.len(), 5);
        assert_eq!(expanded[0].start.date(), NaiveDate::from_ymd_opt(2024, 3, 1).unwrap());
        assert_eq!(expanded[4].start.date(), NaiveDate::from_ymd_opt(2024, 3, 5).unwrap());
    }

    #[test]
    fn test_expand_events_interval_and_count_combined() {
        // Every 3 days, 4 occurrences max
        let combined = RawEvent {
            summary: "Every 3 days [Frank]".to_string(),
            start: NaiveDate::from_ymd_opt(2024, 3, 1).unwrap().and_hms_opt(9, 0, 0).unwrap(),
            end: NaiveDate::from_ymd_opt(2024, 3, 1).unwrap().and_hms_opt(10, 0, 0).unwrap(),
            uid: "uid1".to_string(),
            rrule: Some("FREQ=DAILY;INTERVAL=3;COUNT=4".to_string()),
            exdates: vec![],
            recurrence_id: None,
        };
        let expanded = expand_events(vec![combined]);
        assert_eq!(expanded.len(), 4);
        assert_eq!(expanded[0].start.date(), NaiveDate::from_ymd_opt(2024, 3, 1).unwrap());
        assert_eq!(expanded[1].start.date(), NaiveDate::from_ymd_opt(2024, 3, 4).unwrap());
        assert_eq!(expanded[2].start.date(), NaiveDate::from_ymd_opt(2024, 3, 7).unwrap());
        assert_eq!(expanded[3].start.date(), NaiveDate::from_ymd_opt(2024, 3, 10).unwrap());
    }

    #[test]
    fn test_matches_exclude_filter() {
        let event = Event::new(
            "Meeting with [John Doe]".to_string(),
            NaiveDate::from_ymd_opt(2024, 3, 15).unwrap().and_hms_opt(9, 0, 0).unwrap(),
            NaiveDate::from_ymd_opt(2024, 3, 15).unwrap().and_hms_opt(10, 0, 0).unwrap(),
        );

        // Empty exclude list should include
        assert!(matches_exclude_filter(&event, &[]));

        // Excluding different person should include
        assert!(matches_exclude_filter(&event, &["Jane".to_string()]));

        // Excluding matching person should exclude
        assert!(!matches_exclude_filter(&event, &["John".to_string()]));
        assert!(!matches_exclude_filter(&event, &["john".to_string()])); // case insensitive

        // Multiple exclude filters
        assert!(!matches_exclude_filter(&event, &["Jane".to_string(), "John".to_string()]));

        // No person in event should be included
        let event_no_person = Event::new(
            "Regular meeting".to_string(),
            NaiveDate::from_ymd_opt(2024, 3, 15).unwrap().and_hms_opt(9, 0, 0).unwrap(),
            NaiveDate::from_ymd_opt(2024, 3, 15).unwrap().and_hms_opt(10, 0, 0).unwrap(),
        );
        assert!(matches_exclude_filter(&event_no_person, &["anything".to_string()]));
    }

    #[test]
    fn test_matches_exclude_project_filter() {
        let event = Event::new(
            "Meeting {Project Alpha}".to_string(),
            NaiveDate::from_ymd_opt(2024, 3, 15).unwrap().and_hms_opt(9, 0, 0).unwrap(),
            NaiveDate::from_ymd_opt(2024, 3, 15).unwrap().and_hms_opt(10, 0, 0).unwrap(),
        );

        // Empty exclude list should include
        assert!(matches_exclude_project_filter(&event, &[]));

        // Excluding different project should include
        assert!(matches_exclude_project_filter(&event, &["Beta".to_string()]));

        // Excluding matching project should exclude
        assert!(!matches_exclude_project_filter(&event, &["Alpha".to_string()]));
        assert!(!matches_exclude_project_filter(&event, &["alpha".to_string()])); // case insensitive
        assert!(!matches_exclude_project_filter(&event, &["Project".to_string()])); // partial match

        // No project in event should be included
        let event_no_project = Event::new(
            "Regular meeting".to_string(),
            NaiveDate::from_ymd_opt(2024, 3, 15).unwrap().and_hms_opt(9, 0, 0).unwrap(),
            NaiveDate::from_ymd_opt(2024, 3, 15).unwrap().and_hms_opt(10, 0, 0).unwrap(),
        );
        assert!(matches_exclude_project_filter(&event_no_project, &["anything".to_string()]));
    }

    #[test]
    fn test_expand_events_monthly_recurrence() {
        let monthly = RawEvent {
            summary: "Monthly [Eve]".to_string(),
            start: NaiveDate::from_ymd_opt(2024, 1, 15).unwrap().and_hms_opt(9, 0, 0).unwrap(),
            end: NaiveDate::from_ymd_opt(2024, 1, 15).unwrap().and_hms_opt(10, 0, 0).unwrap(),
            uid: "uid1".to_string(),
            rrule: Some("FREQ=MONTHLY;UNTIL=20240615T235959".to_string()),
            exdates: vec![],
            recurrence_id: None,
        };
        let expanded = expand_events(vec![monthly]);
        // 6 months: Jan, Feb, Mar, Apr, May, Jun
        assert_eq!(expanded.len(), 6);
        assert_eq!(expanded[0].start.date(), NaiveDate::from_ymd_opt(2024, 1, 15).unwrap());
        assert_eq!(expanded[1].start.date(), NaiveDate::from_ymd_opt(2024, 2, 15).unwrap());
        assert_eq!(expanded[5].start.date(), NaiveDate::from_ymd_opt(2024, 6, 15).unwrap());
    }

    #[test]
    fn test_expand_events_monthly_day_overflow() {
        // Test day overflow handling: Jan 31 -> Feb 28 (non-leap year 2023)
        let monthly_31st = RawEvent {
            summary: "Monthly 31st [Frank]".to_string(),
            start: NaiveDate::from_ymd_opt(2023, 1, 31).unwrap().and_hms_opt(9, 0, 0).unwrap(),
            end: NaiveDate::from_ymd_opt(2023, 1, 31).unwrap().and_hms_opt(10, 0, 0).unwrap(),
            uid: "uid1".to_string(),
            rrule: Some("FREQ=MONTHLY;UNTIL=20230430T235959".to_string()),
            exdates: vec![],
            recurrence_id: None,
        };
        let expanded = expand_events(vec![monthly_31st]);
        // 4 months: Jan 31, Feb 28 (clamped), Mar 31, Apr 30 (clamped)
        assert_eq!(expanded.len(), 4);
        assert_eq!(expanded[0].start.date(), NaiveDate::from_ymd_opt(2023, 1, 31).unwrap());
        assert_eq!(expanded[1].start.date(), NaiveDate::from_ymd_opt(2023, 2, 28).unwrap()); // 31st -> 28th
        assert_eq!(expanded[2].start.date(), NaiveDate::from_ymd_opt(2023, 3, 31).unwrap());
        assert_eq!(expanded[3].start.date(), NaiveDate::from_ymd_opt(2023, 4, 30).unwrap()); // 31st -> 30th
    }

    #[test]
    fn test_validate_month_valid() {
        assert!(validate_month(None).is_ok());
        assert!(validate_month(Some(1)).is_ok());
        assert!(validate_month(Some(6)).is_ok());
        assert!(validate_month(Some(12)).is_ok());
    }

    #[test]
    fn test_validate_month_invalid() {
        assert!(validate_month(Some(0)).is_err());
        assert!(validate_month(Some(13)).is_err());
        let err = validate_month(Some(0)).unwrap_err();
        assert!(err.to_string().contains("between 1 and 12"));
    }

    #[test]
    fn test_matches_month_filter() {
        let event = Event::new(
            "Test [Event]".to_string(),
            NaiveDate::from_ymd_opt(2024, 3, 15).unwrap().and_hms_opt(9, 0, 0).unwrap(),
            NaiveDate::from_ymd_opt(2024, 3, 15).unwrap().and_hms_opt(10, 0, 0).unwrap(),
        );
        
        assert!(matches_month_filter(&event, &None));
        assert!(matches_month_filter(&event, &Some(3)));
        assert!(!matches_month_filter(&event, &Some(1)));
        assert!(!matches_month_filter(&event, &Some(12)));
    }

    #[test]
    fn test_week_number_iso() {
        // ISO week numbers - chrono handles these correctly
        let date = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
        assert_eq!(date.iso_week().week(), 1);  // Jan 1, 2024 is week 1
        
        let date = NaiveDate::from_ymd_opt(2024, 3, 15).unwrap();
        assert_eq!(date.iso_week().week(), 11); // March 15, 2024
        
        let date = NaiveDate::from_ymd_opt(2024, 12, 30).unwrap();
        assert_eq!(date.iso_week().week(), 1);  // Dec 30, 2024 is week 1 of 2025
        
        // Verify ISO year at year boundary
        let date = NaiveDate::from_ymd_opt(2024, 12, 30).unwrap();
        assert_eq!(date.iso_week().year(), 2025); // ISO year is 2025
    }

    #[test]
    fn test_weekday_abbrev_to_num() {
        assert_eq!(weekday_abbrev_to_num("MO"), Some(1));
        assert_eq!(weekday_abbrev_to_num("TU"), Some(2));
        assert_eq!(weekday_abbrev_to_num("WE"), Some(3));
        assert_eq!(weekday_abbrev_to_num("TH"), Some(4));
        assert_eq!(weekday_abbrev_to_num("FR"), Some(5));
        assert_eq!(weekday_abbrev_to_num("SA"), Some(6));
        assert_eq!(weekday_abbrev_to_num("SU"), Some(7));
        // Case insensitive
        assert_eq!(weekday_abbrev_to_num("mo"), Some(1));
        assert_eq!(weekday_abbrev_to_num("MO"), Some(1));
        // Only 2-letter abbreviations accepted
        assert_eq!(weekday_abbrev_to_num("Mon"), None);
        assert_eq!(weekday_abbrev_to_num("XX"), None);
        assert_eq!(weekday_abbrev_to_num(""), None);
    }

    #[test]
    fn test_matches_weekday_filter() {
        // Wednesday March 6, 2024
        let wednesday = Event::new(
            "Wednesday meeting [Alice]".to_string(),
            NaiveDate::from_ymd_opt(2024, 3, 6).unwrap().and_hms_opt(9, 0, 0).unwrap(),
            NaiveDate::from_ymd_opt(2024, 3, 6).unwrap().and_hms_opt(10, 0, 0).unwrap(),
        );
        // Friday March 8, 2024
        let friday = Event::new(
            "Friday meeting [Bob]".to_string(),
            NaiveDate::from_ymd_opt(2024, 3, 8).unwrap().and_hms_opt(9, 0, 0).unwrap(),
            NaiveDate::from_ymd_opt(2024, 3, 8).unwrap().and_hms_opt(10, 0, 0).unwrap(),
        );

        // Empty filter includes all
        assert!(matches_weekday_filter(&wednesday, &[]));
        assert!(matches_weekday_filter(&friday, &[]));

        // Single day filter
        assert!(matches_weekday_filter(&wednesday, &["WE".to_string()]));
        assert!(!matches_weekday_filter(&friday, &["WE".to_string()]));

        // Multiple days filter (OR logic)
        assert!(matches_weekday_filter(&wednesday, &["MO".to_string(), "WE".to_string(), "FR".to_string()]));
        assert!(matches_weekday_filter(&friday, &["MO".to_string(), "WE".to_string(), "FR".to_string()]));

        // Case insensitive filter
        assert!(matches_weekday_filter(&wednesday, &["we".to_string()]));

        // Invalid weekday in filter is skipped (valid ones still work)
        assert!(matches_weekday_filter(&wednesday, &["WE".to_string(), "XX".to_string()]));
        // But XX alone doesn't match anyone
        assert!(!matches_weekday_filter(&wednesday, &["XX".to_string()]));
        assert!(!matches_weekday_filter(&friday, &["XX".to_string()]));
    }

    #[test]
    fn test_matches_exclude_weekday_filter() {
        // Wednesday March 6, 2024
        let wednesday = Event::new(
            "Wednesday meeting [Alice]".to_string(),
            NaiveDate::from_ymd_opt(2024, 3, 6).unwrap().and_hms_opt(9, 0, 0).unwrap(),
            NaiveDate::from_ymd_opt(2024, 3, 6).unwrap().and_hms_opt(10, 0, 0).unwrap(),
        );
        // Friday March 8, 2024
        let friday = Event::new(
            "Friday meeting [Bob]".to_string(),
            NaiveDate::from_ymd_opt(2024, 3, 8).unwrap().and_hms_opt(9, 0, 0).unwrap(),
            NaiveDate::from_ymd_opt(2024, 3, 8).unwrap().and_hms_opt(10, 0, 0).unwrap(),
        );

        // Empty exclude list includes all
        assert!(matches_exclude_weekday_filter(&wednesday, &[]));
        assert!(matches_exclude_weekday_filter(&friday, &[]));

        // Excluding a day the event is NOT on keeps it
        assert!(matches_exclude_weekday_filter(&wednesday, &["FR".to_string()]));

        // Excluding a day the event IS on excludes it
        assert!(!matches_exclude_weekday_filter(&wednesday, &["WE".to_string()]));
        assert!(!matches_exclude_weekday_filter(&friday, &["FR".to_string()]));

        // Multiple exclude days (OR logic - excluded if matches ANY)
        assert!(!matches_exclude_weekday_filter(&wednesday, &["MO".to_string(), "WE".to_string(), "FR".to_string()]));

        // Case insensitive
        assert!(!matches_exclude_weekday_filter(&wednesday, &["we".to_string()]));

        // Invalid weekday in exclude list is skipped
        assert!(!matches_exclude_weekday_filter(&wednesday, &["WE".to_string(), "XX".to_string()]));
        assert!(matches_exclude_weekday_filter(&wednesday, &["XX".to_string()])); // only invalid = include
    }

    #[test]
    fn test_matches_exclude_summary_filter() {
        let event = Event::new(
            "Team standup meeting [Alice] {Project}".to_string(),
            NaiveDate::from_ymd_opt(2024, 3, 15).unwrap().and_hms_opt(9, 0, 0).unwrap(),
            NaiveDate::from_ymd_opt(2024, 3, 15).unwrap().and_hms_opt(10, 0, 0).unwrap(),
        );

        // Empty exclude list should include
        assert!(matches_exclude_summary_filter(&event, &[]));

        // Excluding text that appears in summary should exclude
        assert!(!matches_exclude_summary_filter(&event, &["standup".to_string()]));
        assert!(!matches_exclude_summary_filter(&event, &["meeting".to_string()]));
        assert!(!matches_exclude_summary_filter(&event, &["TEAM".to_string()])); // case insensitive
        assert!(!matches_exclude_summary_filter(&event, &["Alice".to_string()])); // partial match

        // Multiple exclude filters (any match excludes)
        assert!(!matches_exclude_summary_filter(&event, &["Alice".to_string(), "xyz".to_string()]));

        // Excluding text that doesn't appear should include
        assert!(matches_exclude_summary_filter(&event, &["vacation".to_string()]));
    }

    #[test]
    fn test_expand_events_yearly_recurrence() {
        let yearly = RawEvent {
            summary: "Yearly [Eve]".to_string(),
            start: NaiveDate::from_ymd_opt(2022, 6, 15).unwrap().and_hms_opt(9, 0, 0).unwrap(),
            end: NaiveDate::from_ymd_opt(2022, 6, 15).unwrap().and_hms_opt(10, 0, 0).unwrap(),
            uid: "uid1".to_string(),
            rrule: Some("FREQ=YEARLY;UNTIL=20251231T235959".to_string()),
            exdates: vec![],
            recurrence_id: None,
        };
        let expanded = expand_events(vec![yearly]);
        // 4 years: 2022, 2023, 2024, 2025
        assert_eq!(expanded.len(), 4);
        assert_eq!(expanded[0].start.date(), NaiveDate::from_ymd_opt(2022, 6, 15).unwrap());
        assert_eq!(expanded[1].start.date(), NaiveDate::from_ymd_opt(2023, 6, 15).unwrap());
        assert_eq!(expanded[2].start.date(), NaiveDate::from_ymd_opt(2024, 6, 15).unwrap());
        assert_eq!(expanded[3].start.date(), NaiveDate::from_ymd_opt(2025, 6, 15).unwrap());
    }

    #[test]
    fn test_expand_events_yearly_leap_day() {
        // Feb 29 on leap years - clamped to Feb 28 on non-leap years
        let leap_day = RawEvent {
            summary: "Leap day meeting [Frank]".to_string(),
            start: NaiveDate::from_ymd_opt(2020, 2, 29).unwrap().and_hms_opt(9, 0, 0).unwrap(),
            end: NaiveDate::from_ymd_opt(2020, 2, 29).unwrap().and_hms_opt(10, 0, 0).unwrap(),
            uid: "uid1".to_string(),
            rrule: Some("FREQ=YEARLY;UNTIL=20251231T235959".to_string()),
            exdates: vec![],
            recurrence_id: None,
        };
        let expanded = expand_events(vec![leap_day]);
        // Feb 29 gets clamped to Feb 28 in non-leap years
        // Limited by 5-year recurrence limit: 2020-2024 = 5 occurrences
        assert_eq!(expanded.len(), 5);
        assert_eq!(expanded[0].start.date(), NaiveDate::from_ymd_opt(2020, 2, 29).unwrap());
        assert_eq!(expanded[1].start.date(), NaiveDate::from_ymd_opt(2021, 2, 28).unwrap());
        assert_eq!(expanded[2].start.date(), NaiveDate::from_ymd_opt(2022, 2, 28).unwrap());
        assert_eq!(expanded[3].start.date(), NaiveDate::from_ymd_opt(2023, 2, 28).unwrap());
        assert_eq!(expanded[4].start.date(), NaiveDate::from_ymd_opt(2024, 2, 29).unwrap());
    }

    #[test]
    fn test_parse_human_duration() {
        use super::parse_human_duration;

        // Hours
        assert_eq!(parse_human_duration("1h"), Some(Duration::hours(1)));
        assert_eq!(parse_human_duration("2h"), Some(Duration::hours(2)));
        assert_eq!(parse_human_duration("1H"), Some(Duration::hours(1))); // case insensitive

        // Minutes
        assert_eq!(parse_human_duration("30m"), Some(Duration::minutes(30)));
        assert_eq!(parse_human_duration("90m"), Some(Duration::minutes(90)));
        assert_eq!(parse_human_duration("30M"), Some(Duration::minutes(30)));

        // Combined
        assert_eq!(parse_human_duration("1h30m"), Some(Duration::hours(1) + Duration::minutes(30)));
        assert_eq!(parse_human_duration("2h15m"), Some(Duration::hours(2) + Duration::minutes(15)));

        // Days
        assert_eq!(parse_human_duration("1d"), Some(Duration::days(1)));
        assert_eq!(parse_human_duration("2d"), Some(Duration::days(2)));

        // Weeks
        assert_eq!(parse_human_duration("1w"), Some(Duration::weeks(1)));

        // Trailing number (treated as minutes)
        assert_eq!(parse_human_duration("45"), Some(Duration::minutes(45)));

        // With spaces
        assert_eq!(parse_human_duration("1h 30m"), Some(Duration::hours(1) + Duration::minutes(30)));

        // Invalid
        assert_eq!(parse_human_duration(""), None);
        assert_eq!(parse_human_duration("xyz"), None);
        assert_eq!(parse_human_duration("1x"), None);
    }

    #[test]
    fn test_matches_duration_filter() {
        let short_event = Event::new(
            "Quick sync [Alice]".to_string(),
            NaiveDate::from_ymd_opt(2024, 3, 15).unwrap().and_hms_opt(9, 0, 0).unwrap(),
            NaiveDate::from_ymd_opt(2024, 3, 15).unwrap().and_hms_opt(9, 15, 0).unwrap(), // 15 min
        );
        let medium_event = Event::new(
            "Standard meeting [Bob]".to_string(),
            NaiveDate::from_ymd_opt(2024, 3, 15).unwrap().and_hms_opt(10, 0, 0).unwrap(),
            NaiveDate::from_ymd_opt(2024, 3, 15).unwrap().and_hms_opt(11, 0, 0).unwrap(), // 1 hour
        );
        let long_event = Event::new(
            "Workshop [Charlie]".to_string(),
            NaiveDate::from_ymd_opt(2024, 3, 15).unwrap().and_hms_opt(13, 0, 0).unwrap(),
            NaiveDate::from_ymd_opt(2024, 3, 15).unwrap().and_hms_opt(17, 0, 0).unwrap(), // 4 hours
        );

        let none_filter: Option<Duration> = None;

        // No filter = all pass
        assert!(matches_duration_filter(&short_event, &none_filter, &none_filter));
        assert!(matches_duration_filter(&medium_event, &none_filter, &none_filter));
        assert!(matches_duration_filter(&long_event, &none_filter, &none_filter));

        // Min duration filter
        let min_1h = Some(Duration::hours(1));
        assert!(!matches_duration_filter(&short_event, &min_1h, &none_filter)); // 15min < 1h
        assert!(matches_duration_filter(&medium_event, &min_1h, &none_filter)); // 1h >= 1h
        assert!(matches_duration_filter(&long_event, &min_1h, &none_filter)); // 4h >= 1h

        // Max duration filter
        let max_2h = Some(Duration::hours(2));
        assert!(matches_duration_filter(&short_event, &none_filter, &max_2h)); // 15min <= 2h
        assert!(matches_duration_filter(&medium_event, &none_filter, &max_2h)); // 1h <= 2h
        assert!(!matches_duration_filter(&long_event, &none_filter, &max_2h)); // 4h > 2h

        // Combined min and max
        let min_30m = Some(Duration::minutes(30));
        let max_3h = Some(Duration::hours(3));
        assert!(!matches_duration_filter(&short_event, &min_30m, &max_3h)); // 15min < 30m
        assert!(matches_duration_filter(&medium_event, &min_30m, &max_3h)); // 1h in range
        assert!(!matches_duration_filter(&long_event, &min_30m, &max_3h)); // 4h > 3h
    }
}
