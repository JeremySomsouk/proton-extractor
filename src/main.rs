use chrono::{Datelike, Duration, Local, NaiveDate, NaiveDateTime};
use clap::{Parser, ValueEnum};
use ical::parser::ical::component::IcalEvent;
use ical::IcalParser;
use std::collections::{BTreeMap, HashSet};
use std::fs::File;
use std::io::BufReader;
use std::path::PathBuf;

const VERSION: &str = env!("CARGO_PKG_VERSION");

#[derive(Debug, Clone, ValueEnum)]
enum MonthFilter {
    Current,
    Previous,
    All,
}

#[derive(Parser, Debug)]
#[command(name = "proton-extractor", about = "Sum calendar event hours from ICS files", version = VERSION)]
struct Args {
    /// Paths to .ics files
    files: Vec<PathBuf>,

    /// Filter by month
    #[arg(short, long, value_enum, default_value = "all")]
    month: MonthFilter,
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
    let clean = value.trim_end_matches('Z');
    NaiveDateTime::parse_from_str(clean, "%Y%m%dT%H%M%S")
        .or_else(|_| {
            NaiveDate::parse_from_str(clean, "%Y%m%d")
                .map(|d| d.and_hms_opt(0, 0, 0).unwrap())
        })
        .ok()
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

            Some(RawEvent {
                summary,
                start: start?,
                end: end?,
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

fn main() {
    let args = Args::parse();

    if args.files.is_empty() {
        eprintln!("Error: no .ics files provided");
        std::process::exit(1);
    }

    let mut all_raw_events = Vec::new();
    for path in &args.files {
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
                    all_raw_events.extend(extract_raw_events(cal.events));
                }
                Err(e) => {
                    eprintln!("Warning: failed to parse {}: {}", path.display(), e);
                }
            }
        }
    }

    let all_events = expand_events(all_raw_events);

    let filtered: Vec<&Event> = all_events
        .iter()
        .filter(|e| matches_filter(e, &args.month))
        .collect();

    if filtered.is_empty() {
        println!("No events found for the selected period.");
        return;
    }

    // Group by year-month
    let mut by_month: BTreeMap<(i32, u32), Vec<&Event>> = BTreeMap::new();
    for event in &filtered {
        let key = (event.start.year(), event.start.month());
        by_month.entry(key).or_default().push(event);
    }

    let mut grand_total_minutes: i64 = 0;

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
            println!("  {:6}  {}", format_hours(mins), event.summary);
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
            println!("  {:6}  {}", format_hours(*mins), person);
        }
    }
}
