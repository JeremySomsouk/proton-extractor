use chrono::{Datelike, Duration, Local, NaiveDate, NaiveDateTime};
use clap::{Parser, ValueEnum};
use ical::parser::ical::component::IcalEvent;
use ical::IcalParser;
use serde::Serialize;
use std::collections::{BTreeMap, HashSet};
use std::fs::File;
use std::io::{self, BufReader};
use std::path::PathBuf;

const VERSION: &str = env!("CARGO_PKG_VERSION");

#[derive(Debug, Clone, ValueEnum)]
enum MonthFilter {
    Current,
    Previous,
    All,
}

#[derive(Debug, Clone, ValueEnum)]
enum OutputFormat {
    Text,
    Json,
    Csv,
    Markdown,
}

#[derive(Parser, Debug)]
#[command(name = "proton-extractor", about = "Sum calendar event hours from ICS files", version = VERSION)]
struct Args {
    /// Paths to .ics files
    files: Vec<PathBuf>,

    /// Filter by month
    #[arg(short, long, value_enum, default_value = "all")]
    month: MonthFilter,

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

    /// Start date (YYYY-MM-DD)
    #[arg(long)]
    from: Option<NaiveDate>,

    /// End date (YYYY-MM-DD)
    #[arg(long)]
    to: Option<NaiveDate>,

    /// Enable verbose output
    #[arg(short, long)]
    verbose: bool,
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
    #[allow(dead_code)]
    duration: Option<Duration>,
    uid: String,
    rrule: Option<String>,
    exdates: Vec<NaiveDate>,
    recurrence_id: Option<NaiveDateTime>,
}

fn parse_ical_datetime(value: &str) -> Option<NaiveDateTime> {
    let clean = value.trim_end_matches('Z');
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

fn parse_rrule(rrule: &str) -> Option<(String, NaiveDateTime)> {
    let mut freq = None;
    let mut until = None;
    for part in rrule.split(';') {
        if let Some(v) = part.strip_prefix("FREQ=") {
            freq = Some(v.to_string());
        } else if let Some(v) = part.strip_prefix("UNTIL=") {
            until = parse_ical_datetime(v);
        }
    }
    Some((freq?, until?))
}

const RECURRENCE_LIMIT_DAYS: i64 = 365 * 5; // 5 year limit for recurrence expansion
const MAX_RECURRENCE_INSTANCES: usize = 365; // Safety limit for instances

fn expand_events(raw_events: Vec<RawEvent>) -> Vec<Event> {
    // Separate overrides (events with RECURRENCE-ID) from base events
    let mut overrides: HashSet<(String, NaiveDate)> = HashSet::new();
    let mut override_events: Vec<RawEvent> = Vec::new();
    let mut base_events: Vec<RawEvent> = Vec::new();

    for event in raw_events {
        if event.recurrence_id.is_some() {
            let rid = event.recurrence_id.unwrap();
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
                let Some((freq, until)) = parse_rrule(rrule) else {
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

                let step = match freq.as_str() {
                    "WEEKLY" => Duration::weeks(1),
                    "DAILY" => Duration::days(1),
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

                let mut current = event.start;
                let mut instances = 0;
                while current <= until {
                    if instances >= MAX_RECURRENCE_INSTANCES {
                        break;
                    }
                    let date = current.date();
                    if !exdate_set.contains(&date)
                        && !overrides.contains(&(event.uid.clone(), date))
                    {
                        result.push(Event::new(event.summary.clone(), current, current + duration));
                    }
                    current += step;
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
                duration,
                uid,
                rrule,
                exdates,
                recurrence_id,
            })
        })
        .collect()
}

fn matches_filter(event: &Event, filter: &MonthFilter) -> bool {
    let now = Local::now().naive_local();
    let (ev_year, ev_month) = (event.start.year(), event.start.month());
    match filter {
        MonthFilter::All => (ev_year, ev_month) <= (now.year(), now.month()),
        MonthFilter::Current => ev_year == now.year() && ev_month == now.month(),
        MonthFilter::Previous => {
            let (y, m) = if now.month() == 1 {
                (now.year() - 1, 12)
            } else {
                (now.year(), now.month() - 1)
            };
            ev_year == y && ev_month == m
        }
    }
}

fn matches_person_filter(event: &Event, person_filter: &Option<String>) -> bool {
    let Some(filter) = person_filter else {
        return true;
    };
    extract_person(&event.summary)
        .map(|p| p.to_lowercase().contains(&filter.to_lowercase()))
        .unwrap_or(false)
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
struct JsonMonthSummary {
    year: i32,
    month: u32,
    month_name: String,
    total_minutes: i64,
    total_formatted: String,
    by_person: Vec<(String, i64, String)>,
    events: Vec<JsonEvent>,
}

#[derive(Serialize)]
struct JsonOutput {
    grand_total_minutes: i64,
    grand_total_formatted: String,
    months: Vec<JsonMonthSummary>,
}

/// Extracts person name from event summary using [name] format.
/// Returns the content inside brackets if found and not empty/whitespace.
fn extract_person(summary: &str) -> Option<&str> {
    let start = summary.rfind('[')?;
    let end = summary.find(']').filter(|&e| e > start)?;
    let inner = &summary[start + 1..end];
    // Validate: not empty and not just whitespace
    if !inner.is_empty() && !inner.trim().is_empty() {
        Some(inner)
    } else {
        None
    }
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
        if !args.exclude_person.is_empty() {
            eprintln!("[verbose] Excluding persons: {:?}", args.exclude_person);
        }
        if let Some(ref f) = args.from {
            eprintln!("[verbose] From date: {}", f);
        }
        if let Some(ref t) = args.to {
            eprintln!("[verbose] To date: {}", t);
        }
    }

    if args.files.is_empty() {
        eprintln!("Error: no .ics files provided");
        std::process::exit(1);
    }

    validate_date_range(&args.from, &args.to)?;

    let mut all_raw_events = Vec::new();
    for path in &args.files {
        if args.verbose {
            eprintln!("[verbose] Reading: {}", path.display());
        }
        let file = match File::open(path) {
            Ok(f) => f,
            Err(e) => {
                eprintln!("Error opening {}: {}", path.display(), e);
                continue;
            }
        };

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

    let filtered: Vec<&Event> = all_events
        .iter()
        .filter(|e| matches_filter(e, &args.month))
        .filter(|e| matches_person_filter(e, &args.person))
        .filter(|e| matches_exclude_filter(e, &args.exclude_person))
        .filter(|e| matches_date_range(e, &args.from, &args.to))
        .collect();

    if args.verbose {
        eprintln!("[verbose] Events after filtering: {}", filtered.len());
    }

    if filtered.is_empty() {
        println!("No events found for the selected period.");
        return Ok(());
    }

    // Group by year-month
    let mut by_month: BTreeMap<(i32, u32), Vec<&Event>> = BTreeMap::new();
    for event in &filtered {
        let key = (event.start.year(), event.start.month());
        by_month.entry(key).or_default().push(event);
    }

    let mut grand_total_minutes: i64 = 0;

    match args.format {
        OutputFormat::Csv => {
            let mut wtr = csv::Writer::from_writer(std::io::stdout());
            wtr.write_record(&["date", "start", "end", "duration_minutes", "person", "summary"])
                .ok();
            for event in &filtered {
                let duration = event.end - event.start;
                let mins = duration.num_minutes();
                if mins <= 0 {
                    continue;
                }
                grand_total_minutes += mins;
                let person = extract_person(&event.summary).unwrap_or("(unknown)").to_string();
                wtr.write_record(&[
                    event.start.format("%Y-%m-%d").to_string(),
                    event.start.format("%H:%M").to_string(),
                    event.end.format("%H:%M").to_string(),
                    mins.to_string(),
                    person,
                    event.summary.clone(),
                ])
                .ok();
            }
            wtr.flush().ok();
            eprintln!("\nGrand Total: {}", format_hours(grand_total_minutes));
        }
        OutputFormat::Json => {
            let mut months_json: Vec<JsonMonthSummary> = Vec::new();
            for ((year, month), events) in &by_month {
                let month_name = chrono::Month::try_from(u8::try_from(*month).unwrap_or(1))
                    .unwrap_or_else(|_| chrono::Month::January)
                    .name()
                    .to_string();
                
                let mut month_minutes: i64 = 0;
                let mut month_by_person: BTreeMap<String, i64> = BTreeMap::new();
                let mut events_json: Vec<JsonEvent> = Vec::new();
                
                for event in events {
                    let duration = event.end - event.start;
                    let mins = duration.num_minutes();
                    if mins <= 0 {
                        continue;
                    }
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
                
                let by_person: Vec<(String, i64, String)> = month_by_person
                    .into_iter()
                    .map(|(p, m)| (p.clone(), m, format_hours(m)))
                    .collect();
                
                months_json.push(JsonMonthSummary {
                    year: *year,
                    month: *month,
                    month_name,
                    total_minutes: month_minutes,
                    total_formatted: format_hours(month_minutes),
                    by_person,
                    events: events_json,
                });
                grand_total_minutes += month_minutes;
            }
            
            let output = JsonOutput {
                grand_total_minutes,
                grand_total_formatted: format_hours(grand_total_minutes),
                months: months_json,
            };
            println!("{}", serde_json::to_string_pretty(&output).unwrap_or_else(|_| "{}".to_string()));
        }
        OutputFormat::Text => {
            for ((year, month), events) in &by_month {
                let month_name = chrono::Month::try_from(u8::try_from(*month).unwrap_or(1))
                    .unwrap_or_else(|_| chrono::Month::January)
                    .name();
                println!("\n--- {} {} ---", month_name, year);

                let mut month_minutes: i64 = 0;
                let mut month_by_person: BTreeMap<&str, i64> = BTreeMap::new();
                for event in events {
                    let duration = event.end - event.start;
                    let mins = duration.num_minutes();
                    if mins <= 0 {
                        continue;
                    }
                    month_minutes += mins;
                    let person = extract_person(&event.summary).unwrap_or("(unknown)");
                    *month_by_person.entry(person).or_default() += mins;
                    if !args.quiet {
                        println!("  {:6}  {}", format_hours(mins), event.summary);
                    }
                }

                println!("  ------");
                for (person, mins) in &month_by_person {
                    println!("  {:6}  {}", format_hours(*mins), person);
                }
                println!("  {:6}  TOTAL", format_hours(month_minutes));
                grand_total_minutes += month_minutes;
            }

            if grand_total_minutes > 0 && by_month.len() > 1 {
                println!("\n=== Grand Total: {} ===", format_hours(grand_total_minutes));
            }

            // Per-person summary
            let mut by_person: BTreeMap<&str, i64> = BTreeMap::new();
            for event in &filtered {
                let mins = (event.end - event.start).num_minutes();
                debug_assert!(mins > 0, "Event with non-positive duration should have been filtered");
                let person = extract_person(&event.summary).unwrap_or("(unknown)");
                *by_person.entry(person).or_default() += mins;
            }

            if !by_person.is_empty() {
                println!("\n=== Hours per person ===");
                for (person, mins) in &by_person {
                    println!("  {:6}  {:>6}  {}", format_hours(*mins), format_percentage(*mins, grand_total_minutes), person);
                }
            }
        }
        OutputFormat::Markdown => {
            println!("# Calendar Hours Report\n");

            for ((year, month), events) in &by_month {
                let month_name = chrono::Month::try_from(u8::try_from(*month).unwrap_or(1))
                    .unwrap_or_else(|_| chrono::Month::January)
                    .name();

                let mut month_minutes: i64 = 0;
                let mut month_by_person: BTreeMap<&str, i64> = BTreeMap::new();

                for event in events {
                    let duration = event.end - event.start;
                    let mins = duration.num_minutes();
                    if mins <= 0 {
                        continue;
                    }
                    month_minutes += mins;
                    let person = extract_person(&event.summary).unwrap_or("(unknown)");
                    *month_by_person.entry(person).or_default() += mins;
                }

                println!("## {} {}\n", month_name, year);
                println!("| Hours | Person |");
                println!("|-------|--------|");
                for (person, mins) in &month_by_person {
                    println!("| {} | {} |", format_hours(*mins), person);
                }
                println!("| **{}** | **TOTAL** |", format_hours(month_minutes));
                println!();
                grand_total_minutes += month_minutes;
            }

            if grand_total_minutes > 0 && by_month.len() > 1 {
                println!("---\n\n**Grand Total: {}**\n", format_hours(grand_total_minutes));
            }

            // Per-person summary across all months
            let mut by_person: BTreeMap<&str, i64> = BTreeMap::new();
            for event in &filtered {
                let mins = (event.end - event.start).num_minutes();
                debug_assert!(mins > 0, "Event with non-positive duration should have been filtered");
                let person = extract_person(&event.summary).unwrap_or("(unknown)");
                *by_person.entry(person).or_default() += mins;
            }

            if !by_person.is_empty() {
                println!("## Hours per Person\n");
                println!("| Hours | % | Person |");
                println!("|-------|-------|--------|");
                for (person, mins) in &by_person {
                    println!("| {} | {} | {} |", format_hours(*mins), format_percentage(*mins, grand_total_minutes), person);
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
    fn test_parse_rrule() {
        assert!(parse_rrule("FREQ=WEEKLY;UNTIL=20240315T090000Z").is_some());
        assert_eq!(parse_rrule("FREQ=DAILY"), None); // missing UNTIL
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
}
