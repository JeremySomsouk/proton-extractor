use chrono::{Datelike, Local, NaiveDateTime};
use clap::{Parser, ValueEnum};
use ical::parser::ical::component::IcalEvent;
use ical::IcalParser;
use std::collections::BTreeMap;
use std::fs::File;
use std::io::BufReader;
use std::path::PathBuf;

#[derive(Debug, Clone, ValueEnum)]
enum MonthFilter {
    Current,
    Previous,
    All,
}

#[derive(Parser, Debug)]
#[command(name = "proton-extractor", about = "Sum calendar event hours from ICS files")]
struct Args {
    /// Paths to .ics files
    files: Vec<PathBuf>,

    /// Filter by month
    #[arg(short, long, value_enum, default_value = "all")]
    month: MonthFilter,
}

struct Event {
    summary: String,
    start: NaiveDateTime,
    end: NaiveDateTime,
}

fn parse_ical_datetime(value: &str) -> Option<NaiveDateTime> {
    let clean = value.trim_end_matches('Z');
    NaiveDateTime::parse_from_str(clean, "%Y%m%dT%H%M%S")
        .or_else(|_| {
            chrono::NaiveDate::parse_from_str(clean, "%Y%m%d")
                .map(|d| d.and_hms_opt(0, 0, 0).unwrap())
        })
        .ok()
}

fn extract_events(ical_events: Vec<IcalEvent>) -> Vec<Event> {
    ical_events
        .into_iter()
        .filter_map(|e| {
            let mut summary = String::from("(untitled)");
            let mut start = None;
            let mut end = None;

            for prop in &e.properties {
                let val = match &prop.value {
                    Some(v) => v.as_str(),
                    None => continue,
                };
                match prop.name.as_str() {
                    "SUMMARY" => summary = val.to_string(),
                    "DTSTART" => start = parse_ical_datetime(val),
                    "DTEND" => end = parse_ical_datetime(val),
                    _ => {}
                }
            }

            Some(Event {
                summary,
                start: start?,
                end: end?,
            })
        })
        .collect()
}

fn matches_filter(event: &Event, filter: &MonthFilter) -> bool {
    let now = Local::now().naive_local();
    match filter {
        MonthFilter::All => true,
        MonthFilter::Current => {
            event.start.year() == now.year() && event.start.month() == now.month()
        }
        MonthFilter::Previous => {
            let (y, m) = if now.month() == 1 {
                (now.year() - 1, 12)
            } else {
                (now.year(), now.month() - 1)
            };
            event.start.year() == y && event.start.month() == m
        }
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

    let mut all_events = Vec::new();
    for path in &args.files {
        let file = File::open(path).unwrap_or_else(|e| {
            eprintln!("Error opening {}: {}", path.display(), e);
            std::process::exit(1);
        });

        let reader = BufReader::new(file);
        let parser = IcalParser::new(reader);

        for calendar in parser {
            match calendar {
                Ok(cal) => {
                    all_events.extend(extract_events(cal.events));
                }
                Err(e) => {
                    eprintln!("Warning: failed to parse {}: {}", path.display(), e);
                }
            }
        }
    }

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
        let month_name = chrono::Month::try_from(u8::try_from(*month).unwrap())
            .unwrap()
            .name();
        println!("\n--- {} {} ---", month_name, year);

        let mut month_minutes: i64 = 0;
        for event in events {
            let duration = event.end - event.start;
            let mins = duration.num_minutes();
            if mins <= 0 {
                continue;
            }
            month_minutes += mins;
            println!("  {:6}  {}", format_hours(mins), event.summary);
        }

        println!("  ------");
        println!("  {:6}  TOTAL", format_hours(month_minutes));
        grand_total_minutes += month_minutes;
    }

    if by_month.len() > 1 {
        println!("\n=== Grand Total: {} ===", format_hours(grand_total_minutes));
    }
}