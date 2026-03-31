use chrono::{Datelike, Duration, Local, NaiveDate, NaiveDateTime, Timelike};
use clap::{CommandFactory, Parser, ValueEnum};
use ical::parser::ical::component::IcalEvent;
use ical::IcalParser;
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::fs::File;
use std::io::{self, BufReader, Write};
use std::path::{Path, PathBuf};
use tracing::{debug, Level};
use tracing_subscriber::FmtSubscriber;

const VERSION: &str = env!("CARGO_PKG_VERSION");

// Terminal colors - automatically disabled if not a TTY or when --no-color is set
mod color {
    use std::fmt;
    use std::io::IsTerminal;
    use std::sync::atomic::{AtomicBool, Ordering};

    static NO_COLOR: AtomicBool = AtomicBool::new(false);

    pub fn set_no_color(val: bool) {
        NO_COLOR.store(val, Ordering::SeqCst);
    }

    pub fn is_color_enabled() -> bool {
        if NO_COLOR.load(Ordering::SeqCst) {
            return false;
        }
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
    pub const RED: Color = Color(31);
    pub const DIM: Color = Color(2);
}

fn colored<S: AsRef<str>>(c: color::Color, text: S) -> String {
    format!("{}{}{}", c, text.as_ref(), c)
}

/// Styled error message output - clear, actionable, and consistent
/// Accepts format args for cleaner error construction
fn print_error<S: AsRef<str>>(msg: S) {
    eprintln!("{} {}", colored(color::RED, "error:"), msg.as_ref());
}

/// Styled warning message output - non-blocking feedback
fn print_warn<S: AsRef<str>>(msg: S) {
    eprintln!("{} {}", colored(color::YELLOW, "warning:"), msg.as_ref());
}

/// Styled success message output - for simple success cases
/// Use for single-line success messages without counts
fn print_success<S: AsRef<str>>(msg: S) {
    println!("{} {}", colored(color::GREEN, "✓"), msg.as_ref());
}

/// Styled success with count - for export operations
fn print_exported(count: usize, path: &Path) {
    let event_label = if count == 1 { "event" } else { "events" };
    println!(
        "{} Exported {} {} → {}",
        colored(color::GREEN, "✓"),
        colored(color::YELLOW, count.to_string()),
        event_label,
        colored(color::CYAN, path.display().to_string())
    );
}

/// Styled hint message output - helpful suggestions with user's specific values
/// Use for actionable tips after errors
fn print_hint<S: AsRef<str>>(msg: S) {
    eprintln!("{} {}", colored(color::DIM, "hint:"), colored(color::CYAN, msg.as_ref()));
}

/// Multiple hints at once - accepts slice of &str
fn print_hints(hints: &[&str]) {
    for hint in hints {
        eprintln!("    {} {}", colored(color::DIM, "→"), colored(color::CYAN, *hint));
    }
}

/// Styled notice message output - neutral notices that aren't errors
/// Used when there's nothing to report but it's not a failure
fn print_notice<S: AsRef<str>>(msg: S) {
    eprintln!("{} {}", colored(color::YELLOW, "○"), msg.as_ref());
}

/// Styled info message for successful list operations
fn print_list_summary(count: usize, label: &str) {
    let item = if count == 1 { label } else { &format!("{}s", label) };
    eprintln!(
        "{} {} {} found",
        colored(color::GREEN, "✓"),
        colored(color::BOLD, count.to_string()),
        item
    );
}

/// Prompt user for confirmation in interactive mode.
/// Returns false immediately in non-interactive (piped) mode to avoid hanging.
/// Uses [Y/n] convention - uppercase Y means Yes is the default.
fn confirm(prompt: &str) -> bool {
    // In non-interactive mode, fail safely instead of hanging
    if !atty::is(atty::Stream::Stdin) {
        eprintln!(
            "{} {} {}",
            colored(color::YELLOW, "warning:"),
            "Cannot prompt in non-interactive mode",
            colored(color::CYAN, "(use --yes or --force)")
        );
        return false;
    }

    eprint!("  {} {} [{}/n] ", colored(color::CYAN, "?"), prompt, colored(color::GREEN, "Y"));
    io::stderr().flush().ok();
    let mut response = String::new();
    if io::stdin().read_line(&mut response).is_err() {
        return false;
    }
    let response = response.trim().to_lowercase();
    // Empty response or 'y'/'yes' confirms (Y is default)
    response.is_empty() || response.eq("y") || response.eq("yes")
}

/// Animated progress indicator for long-running operations
struct Spinner {
    message: String,
    chars: Vec<char>,
    current: usize,
}

impl Spinner {
    fn new(message: &str) -> Self {
        Spinner {
            message: message.to_string(),
            chars: vec!['⠋', '⠙', '⠹', '⠸', '⠼', '⠴', '⠦', '⠧', '⠇', '⠏'],
            current: 0,
        }
    }

    fn tick(&mut self) {
        // Carriage return + clear line + print new spinner + message
        eprint!("\r\x1b[K{}{}", self.chars[self.current], self.message);
        io::stderr().flush().ok();
        self.current = (self.current + 1) % self.chars.len();
    }

    fn finish(&self) {
        // Clear the current line completely
        eprint!("\r\x1b[K\r");
        io::stderr().flush().ok();
    }

    /// Finish with a success message
    fn finish_with_success(&self, message: &str) {
        self.finish();
        println!("{} {}", colored(color::GREEN, "✓"), message);
    }

    /// Finish with an error message (consistent with print_error style)
    fn finish_with_error(&self, message: &str) {
        self.finish();
        eprintln!("{} {}", colored(color::RED, "error:"), message);
    }

}



#[derive(Debug, Clone, ValueEnum)]
enum DateFilter {
    Current,
    Previous,
    All,
    Today,
    Yesterday,
    Tomorrow,
    Week,
    LastWeek,
}

#[derive(Debug, Clone, ValueEnum)]
enum SortBy {
    Date,
    Duration,
    Person,
    Project,
    Start,
    End,
}

#[derive(Debug, Clone, ValueEnum)]
enum OutputFormat {
    Text,
    Json,
    Jsonl,
    Csv,
    Markdown,
    Ical,
    Html,
    Yaml,
    Toml,
    Pivot,
}

#[derive(Debug, Clone, ValueEnum)]
enum StatsFormat {
    Text,
    Json,
    Yaml,
}

#[derive(Debug, Clone, ValueEnum, PartialEq)]
enum EventStatus {
    Confirmed,
    Tentative,
    Cancelled,
}

impl std::fmt::Display for EventStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EventStatus::Confirmed => write!(f, "CONFIRMED"),
            EventStatus::Tentative => write!(f, "TENTATIVE"),
            EventStatus::Cancelled => write!(f, "CANCELLED"),
        }
    }
}

#[derive(Parser, Debug)]
#[command(name = "proton-extractor", about = "Sum calendar event hours from ICS files", version = VERSION)]
#[command(long_about = "Sum calendar event hours from ICS files

proton-extractor parses .ics calendar files, extracts events with [person] tags and {project} tags,
and computes total hours worked per person. Supports filtering, grouping, multiple output formats
(text, JSON, CSV, HTML, YAML, TOML, iCal), and statistics.

Enable shell completion for faster CLI usage:
  source <(proton-extractor --generate-completion bash)   # bash
  source <(proton-extractor --generate-completion zsh)    # zsh
  proton-extractor --generate-completion fish | source   # fish")]
#[command(after_help = "EXAMPLES:
  # ── Quick Start ──────────────────────────────────────────────────────────────
  proton-extractor calendar.ics                              # Basic usage
  proton-extractor --stdin < calendar.ics                # Pipe ICS content

  # ── Filtering ───────────────────────────────────────────────────────────────
  proton-extractor calendar.ics --person \"Alice\"             # By person
  proton-extractor calendar.ics --project \"Backend\"          # By project
  proton-extractor calendar.ics --tag \"urgent\"              # By tag (person OR project)
  proton-extractor calendar.ics -d week                       # This week's events
  proton-extractor calendar.ics --yesterday                   # Yesterday
  proton-extractor calendar.ics --from 2024-01-01 --to 2024-03-31  # Date range
  proton-extractor calendar.ics --week-number W10             # ISO week (current year)
  proton-extractor calendar.ics --week-number 2024-W10        # ISO week (specific year)
  proton-extractor calendar.ics --start-after 09:00 --end-before 17:00  # Business hours
  proton-extractor calendar.ics --min-duration 30m --max-duration 2h  # Duration range
  proton-extractor calendar.ics --recent 7                    # Last 7 days
  proton-extractor calendar.ics --weekdays MO,WE,FR          # Specific weekdays
  proton-extractor calendar.ics --exclude-recurring          # Skip recurring events

  # ── Output Modes ─────────────────────────────────────────────────────────────
  proton-extractor calendar.ics -q                           # Quiet (totals only)
  proton-extractor calendar.ics --total-only                 # Single line total (scripts)
  proton-extractor calendar.ics -s                           # Statistics summary
  proton-extractor calendar.ics --dry-run                    # Preview without output

  # ── Listing & Discovery ──────────────────────────────────────────────────────
  proton-extractor calendar.ics -P                           # List all persons
  proton-extractor calendar.ics -J                           # List all projects
  proton-extractor calendar.ics -E                           # List all events
  proton-extractor calendar.ics -L                           # List all locations
  proton-extractor calendar.ics -T                           # List all tags

  # ── Grouping ─────────────────────────────────────────────────────────────────
  proton-extractor calendar.ics --group-by-person            # Group by person
  proton-extractor calendar.ics --group-by-project           # Group by project
  proton-extractor calendar.ics --group-by-weekday           # Group by weekday
  proton-extractor calendar.ics --group-by-location          # Group by location

  # ── Top/Bottom Events ────────────────────────────────────────────────────────
  proton-extractor calendar.ics --top 10                     # Top 10 longest events
  proton-extractor calendar.ics --bottom 5                   # Bottom 5 shortest events

  # ── Sorting ─────────────────────────────────────────────────────────────────
  proton-extractor calendar.ics --sort-by duration --reverse  # Longest first
  proton-extractor calendar.ics --sort-by person             # Alphabetical by person

  # ── Export ───────────────────────────────────────────────────────────────────
  proton-extractor calendar.ics -f csv -o report.csv         # CSV export
  proton-extractor calendar.ics -f json -o report.json       # JSON export (pretty)
  proton-extractor calendar.ics -f json --compact -o r.json  # JSON export (compact)
  proton-extractor calendar.ics -f html -o report.html       # HTML report
  proton-extractor calendar.ics -f pivot -o pivot.txt        # Pivot table
  proton-extractor calendar.ics -f yaml -o report.yaml       # YAML export
  proton-extractor calendar.ics -f toml -o report.toml       # TOML export
  proton-extractor calendar.ics -f markdown -o report.md     # Markdown table
  proton-extractor calendar.ics -f ical -o events.ics        # iCalendar format
  # Pipe to jq: proton-extractor -f json | jq '.grand_total_minutes'
  # Formats: text, json, jsonl, csv, markdown, ical, html, yaml, toml, pivot

  # ── Convenience ──────────────────────────────────────────────────────────────
  proton-extractor calendar.ics --yes                         # Auto-confirm overwrite
  proton-extractor --validate [args]                          # Validate without running
  proton-extractor --generate-completion bash | source        # Shell completion

  # ── Multiple Files ───────────────────────────────────────────────────────────
  proton-extractor calendar1.ics calendar2.ics -O ./output/     # Batch process

TIP: Use --validate to check your arguments before running in CI/CD pipelines.
TIP: Use --total-only for clean numeric output in scripts: $(proton-extractor -q -d today)
TIP: Use -q (shorthand) instead of --quiet for faster typing
TIP: Use --no-color for clean output in files/logs (or pipe to 'cat')")]
#[command(version = VERSION)]
struct Args {
    /// Paths to .ics files
    files: Vec<PathBuf>,

    /// Filter by date: current month, previous, all, or today
    #[arg(short = 'd', long, value_enum, default_value = "all")]
    date: DateFilter,

    /// Filter by a specific year (e.g., 2024)
    #[arg(long)]
    year: Option<i32>,

    /// Filter by a specific month (1-12, requires --year)
    #[arg(long, requires = "year")]
    month: Option<u32>,

    /// Only show totals, hide individual events
    #[arg(short = 'q', long)]
    quiet: bool,

    /// Produce no output at all (useful for cron/CI with exit code only)
    #[arg(long)]
    silent: bool,

    /// Output format
    #[arg(short, long, value_enum, default_value = "text")]
    format: OutputFormat,

    /// Output JSON format (shorthand for --format json)
    #[arg(short = 'j', long = "json", hide = true)]
    json_format: bool,

    /// Exclude events matching this person name (case-insensitive, can be repeated)
    #[arg(long)]
    exclude_person: Vec<String>,

    /// Filter by person name (case-insensitive)
    #[arg(long)]
    person: Option<String>,

    /// Filter by any of these persons (OR logic, case-insensitive, can be repeated)
    #[arg(long, value_delimiter = ',', value_name = "PERSONS")]
    persons: Option<Vec<String>>,

    /// Filter by project name in {project} tags (case-insensitive)
    #[arg(long)]
    project: Option<String>,

    /// Filter by tag (matches both [person] OR {project}, case-insensitive)
    #[arg(long)]
    tag: Option<String>,

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

    /// Show only the grand total (single line output, useful for scripting)
    #[arg(long, conflicts_with_all = ["quiet", "sum_only", "list_persons", "list_projects", "list_events", "list_locations", "list_categories", "list_tags", "list_years", "list_uids", "stats", "top", "bottom", "group_by_person", "group_by_project", "group_by_weekday", "group_by_location", "group_by_category", "dry_run", "silent"])]
    total_only: bool,

    /// Force overwrite of output file without confirmation
    #[arg(short = 'y', long = "yes", visible_alias = "force")]
    yes: bool,

    /// Output file path (default: stdout)
    #[arg(short = 'o', long)]
    output: Option<PathBuf>,

    /// Output directory for batch processing (creates one file per input file)
    #[arg(short = 'O', long)]
    output_dir: Option<PathBuf>,

    /// List all unique persons found in events
    #[arg(short = 'P', long)]
    list_persons: bool,

    /// List all unique projects found in events
    #[arg(short = 'J', long)]
    list_projects: bool,

    /// List all unique events found (one per line with date and summary)
    #[arg(short = 'E', long)]
    list_events: bool,

    /// List all unique locations found in events
    #[arg(short = 'L', long)]
    list_locations: bool,

    /// List all unique categories found in events
    #[arg(long)]
    list_categories: bool,

    /// Filter by category name (case-insensitive)
    #[arg(long)]
    category: Option<String>,

    /// Exclude events matching this category (case-insensitive, can be repeated)
    #[arg(long)]
    exclude_category: Vec<String>,

    /// Filter by location (case-insensitive)
    #[arg(long)]
    location: Option<String>,

    /// Exclude events matching this location (case-insensitive, can be repeated)
    #[arg(long)]
    exclude_location: Vec<String>,

    /// Exclude recurring events (events with RRULE)
    #[arg(long)]
    exclude_recurring: bool,

    /// Only show recurring events (events with RRULE)
    #[arg(long, conflicts_with_all = ["exclude_recurring"])]
    include_recurring: bool,

    /// Filter by event status: CONFIRMED, TENTATIVE, or CANCELLED (case-insensitive)
    #[arg(long, value_enum)]
    status: Option<EventStatus>,

    /// Exclude events with this status (case-insensitive, can be repeated)
    #[arg(long)]
    exclude_status: Vec<EventStatus>,

    /// Generate shell completion script for bash, zsh, fish, or powershell
    #[arg(long, value_enum)]
    generate_completion: Option<clap_complete::Shell>,

    /// Preview mode: show event count without processing output
    #[arg(short = 'n', long)]
    dry_run: bool,

    /// Filter by day of week: MO,TU,WE,TH,FR,SA,SU (can be repeated, e.g., --weekdays MO --weekdays WE)
    #[arg(long = "weekdays", alias = "weekday", value_delimiter = ',', value_name = "DAYS")]
    weekdays: Option<Vec<String>>,

    /// Exclude events on these days of week: MO,TU,WE,TH,FR,SA,SU (can be repeated, complements --weekdays)
    #[arg(long = "exclude-weekdays", alias = "exclude-weekday", value_delimiter = ',', value_name = "DAYS")]
    exclude_weekdays: Option<Vec<String>>,

    /// Exclude events whose summary contains this text (case-insensitive, can be repeated)
    #[arg(long)]
    exclude_summary: Vec<String>,

    /// Filter events whose summary contains this text (case-insensitive, can be repeated)
    #[arg(long)]
    search: Vec<String>,

    /// Include events whose summary contains this text (case-insensitive, can be repeated)
    /// Useful for finding specific types of events within larger sets
    #[arg(long)]
    include_summary: Vec<String>,

    /// Filter events that start at or after this time (HH:MM format, e.g., "09:00" or "17:30")
    #[arg(long)]
    start_after: Option<String>,

    /// Filter events that start at or before this time (HH:MM format, e.g., "09:00" or "17:30")
    #[arg(long)]
    start_before: Option<String>,

    /// Filter events that end at or after this time (HH:MM format, e.g., "09:00" or "17:30")
    #[arg(long)]
    end_after: Option<String>,

    /// Filter events that end at or before this time (HH:MM format, e.g., "09:00" or "17:30")
    #[arg(long)]
    end_before: Option<String>,

    /// Enable compact JSON/YAML output (single line, no pretty-printing)
    #[arg(long)]
    compact: bool,

    /// Show statistics about events (count, avg/day, top person, busiest day)
    #[arg(short = 's', long)]
    stats: bool,

    /// Output format for statistics (only applies when --stats is used)
    #[arg(long, value_enum, default_value = "text")]
    stats_format: StatsFormat,

    /// Reverse chronological order (newest first)
    #[arg(long)]
    reverse: bool,

    /// Group output by person instead of by month
    #[arg(long)]
    group_by_person: bool,

    /// Group output by project instead of by month
    #[arg(long)]
    group_by_project: bool,

    /// Group output by day of week instead of by month
    #[arg(long)]
    group_by_weekday: bool,

    /// Group output by location instead of by month
    #[arg(long)]
    group_by_location: bool,

    /// Group output by category instead of by month
    #[arg(long)]
    group_by_category: bool,

    /// Group output by year instead of by month
    #[arg(long)]
    group_by_year: bool,

    /// Filter by ISO week number (1-53), optionally with year (e.g., "10" or "2024-W10")
    #[arg(long, alias = "iso-week")]
    week_number: Option<String>,

    /// Limit output to N events (useful for large datasets)
    #[arg(long)]
    limit: Option<usize>,

    /// Show top N events by duration (useful for finding longest meetings)
    #[arg(long)]
    top: Option<usize>,

    /// Show bottom N events by duration (useful for finding shortest/phantom meetings)
    #[arg(long, conflicts_with = "top")]
    bottom: Option<usize>,

    /// Quick filter: show only today's events
    #[arg(short = 't', long)]
    today: bool,

    /// Quick filter: show only yesterday's events
    #[arg(long)]
    yesterday: bool,

    /// Quick filter: show only tomorrow's events
    #[arg(short = 'm', long)]
    tomorrow: bool,

    /// Quick filter: show only this week's events (Monday to Sunday)
    #[arg(short = 'w', long)]
    weekly: bool,

    /// Quick filter: show only last week's events (Monday to Sunday)
    #[arg(short = 'l', long)]
    last_week: bool,

    /// Show events from the last N days (including today)
    #[arg(long)]
    recent: Option<u32>,

    /// Filter out events shorter than this duration (e.g., "30m", "1h", "2h30m")
    #[arg(long)]
    min_duration: Option<String>,

    /// Filter out events longer than this duration (e.g., "8h", "4h30m")
    #[arg(long)]
    max_duration: Option<String>,

    /// Disable colored output
    #[arg(long)]
    no_color: bool,

    /// Read from stdin instead of files (useful for piping ICS content)
    #[arg(long)]
    stdin: bool,

    /// List all unique tags found in events (shows [person] and {project} separately)
    #[arg(short = 'T', long)]
    list_tags: bool,

    /// List all unique years found in events
    #[arg(long)]
    list_years: bool,

    /// List all unique UIDs found in events (useful for debugging/analysis)
    #[arg(long)]
    list_uids: bool,

    /// Remove duplicate events (same summary, start, and end time)
    #[arg(long)]
    dedupe: bool,

    /// Remove duplicate events by summary only (keeps first occurrence)
    #[arg(long)]
    dedupe_by_summary: bool,

    /// Show only events without [person] or {project} tags (untagged events)
    #[arg(long)]
    only_untagged: bool,

    /// Sort events by: date (default), duration, person, project, start time, end time
    #[arg(long, value_enum, default_value = "date")]
    sort_by: SortBy,

    /// Reverse the sort order (use with --sort-by)
    #[arg(long)]
    sort_reverse: bool,

    /// Validate arguments and exit (useful for CI/CD pre-flight checks)
    #[arg(long)]
    validate: bool,
}

fn validate_date_range(from: &Option<NaiveDate>, to: &Option<NaiveDate>) -> io::Result<()> {
    if let (Some(from_date), Some(to_date)) = (from, to) {
        if from_date > to_date {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "--from ({}) must be before or equal to --to ({})",
                    from_date, to_date
                ),
            ));
        }
        // Warn if date range is very large (> 2 years)
        let days_diff = (*to_date - *from_date).num_days();
        if days_diff > 730 {
            debug!("Large date range detected: {} days", days_diff);
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

fn validate_time_filter(time_str: &str, flag_name: &str) -> io::Result<()> {
    if let Some((hours, minutes)) = parse_time(time_str) {
        if hours > 23 || minutes > 59 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "invalid time '{}' for --{}: hours must be 0-23, minutes must be 0-59",
                    time_str, flag_name
                ),
            ));
        }
        Ok(())
    } else {
        // Provide actionable hint for common mistakes
        let hint: &str = if time_str.len() == 4 && time_str.chars().all(|c| c.is_ascii_digit()) {
            "did you mean ':' separator? (e.g., '09:00')"
        } else if time_str.contains('.') {
            "use ':' as separator (e.g., '09:00' not '09.00')"
        } else if time_str.len() < 4 {
            "time format should be HH:MM (e.g., '09:00' or '17:30')"
        } else {
            "use HH:MM format (24-hour)"
        };

        Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "invalid time '{}' for --{}: {}\n    {} Valid format: HH:MM (e.g., '09:00', '17:30', '23:59')",
                time_str, flag_name, hint, colored(color::DIM, "→")
            ),
        ))
    }
}

/// Validate week number format with actionable suggestions
fn validate_week_number(week_str: &Option<String>) -> io::Result<()> {
    if let Some(ref week) = week_str {
        if parse_week_filter(week).is_none() {
            let hint = if week.chars().all(|c| c.is_ascii_digit()) && week.len() <= 2 {
                format!("did you mean 'W{}'? (add 'W' prefix for ISO week)", week)
            } else if week.starts_with("W") && week.len() <= 3 {
                format!("did you mean '{}'? (include year, e.g., 2024-{})", week, week)
            } else {
                String::new()
            };

            let msg = if hint.is_empty() {
                format!(
                    "invalid week format '{}': expected W10, 10, or 2024-W10",
                    week
                )
            } else {
                format!(
                    "invalid week format '{}': expected W10, 10, or 2024-W10\n    {}",
                    week, hint
                )
            };

            return Err(io::Error::new(io::ErrorKind::InvalidInput, msg));
        }
    }
    Ok(())
}

/// Validate weekday abbreviations with actionable suggestions for common mistakes
fn validate_weekdays(weekdays: &Option<Vec<String>>, flag_name: &str) -> io::Result<()> {
    if let Some(ref days) = weekdays {
        let valid_abbrevs = ["MO", "TU", "WE", "TH", "FR", "SA", "SU"];
        let valid_full = ["MONDAY", "TUESDAY", "WEDNESDAY", "THURSDAY", "FRIDAY", "SATURDAY", "SUNDAY"];

        for day in days {
            let upper = day.to_uppercase();
            // Check if it's a valid abbreviation
            if !valid_abbrevs.contains(&upper.as_str()) {
                // Check if it's a full day name (common mistake)
                if valid_full.contains(&upper.as_str()) {
                    let suggested = valid_abbrevs[valid_full.iter().position(|&d| d == upper).unwrap()];
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!(
                            "invalid weekday '{}' for --{}: use abbreviation '{}' (not '{}')",
                            day, flag_name, suggested, day
                        ),
                    ));
                }

                // Check for common typos
                let typo_suggestions: Vec<&str> = valid_abbrevs.iter()
                    .filter(|&&abbr| {
                        // Check Levenshtein distance of 1 or same starting letter
                        abbr.chars().next() == upper.chars().next()
                        || levenshtein_distance(&upper.to_lowercase(), &abbr.to_lowercase()) <= 2
                    })
                    .copied()
                    .collect();

                let msg = if !typo_suggestions.is_empty() {
                    format!(
                        "invalid weekday '{}' for --{}: valid values are {}{}",
                        day,
                        flag_name,
                        valid_abbrevs.join(", "),
                        if typo_suggestions.len() == 1 {
                            format!(" (did you mean '{}'?)", typo_suggestions[0])
                        } else {
                            String::new()
                        }
                    )
                } else {
                    format!(
                        "invalid weekday '{}' for --{}: valid values are {}",
                        day, flag_name, valid_abbrevs.join(", ")
                    )
                };

                return Err(io::Error::new(io::ErrorKind::InvalidInput, msg));
            }
        }
    }
    Ok(())
}

/// Calculate Levenshtein distance between two strings (simple implementation)
fn levenshtein_distance(s1: &str, s2: &str) -> usize {
    let s1_chars: Vec<char> = s1.chars().collect();
    let s2_chars: Vec<char> = s2.chars().collect();
    let len1 = s1_chars.len();
    let len2 = s2_chars.len();

    if len1 == 0 { return len2; }
    if len2 == 0 { return len1; }

    let mut matrix = vec![vec![0usize; len2 + 1]; len1 + 1];

    for i in 0..=len1 { matrix[i][0] = i; }
    for j in 0..=len2 { matrix[0][j] = j; }

    for i in 1..=len1 {
        for j in 1..=len2 {
            let cost = if s1_chars[i-1] == s2_chars[j-1] { 0 } else { 1 };
            matrix[i][j] = std::cmp::min(
                std::cmp::min(matrix[i-1][j] + 1, matrix[i][j-1] + 1),
                matrix[i-1][j-1] + cost
            );
        }
    }

    matrix[len1][len2]
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
            format!(
                "File '{}' has invalid extension '.{}'. Expected '.ics' file",
                path.display(),
                ext
            ),
        )),
        None => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "File '{}' has no file extension. Expected '.ics' file",
                path.display()
            ),
        )),
    }
}

#[derive(Clone)]
struct Event {
    summary: String,
    start: NaiveDateTime,
    end: NaiveDateTime,
    uid: Option<String>,
    location: Option<String>,
    categories: Vec<String>,
    is_recurring: bool,
    status: Option<String>,
    source_file: Option<String>,
}

impl Event {
    #[allow(dead_code)]
    fn new(summary: String, start: NaiveDateTime, end: NaiveDateTime) -> Self {
        Self {
            summary,
            start,
            end,
            uid: None,
            location: None,
            categories: vec![],
            is_recurring: false,
            status: None,
            source_file: None,
        }
    }

    fn with_recurring(
        summary: String,
        start: NaiveDateTime,
        end: NaiveDateTime,
        location: Option<String>,
        categories: Vec<String>,
        is_recurring: bool,
    ) -> Self {
        Self {
            summary,
            start,
            end,
            uid: None,
            location,
            categories,
            is_recurring,
            status: None,
            source_file: None,
        }
    }

    fn with_source(mut self, source: String) -> Self {
        self.source_file = Some(source);
        self
    }

    fn with_uid(mut self, uid: String) -> Self {
        self.uid = Some(uid);
        self
    }

    fn with_status(mut self, status: Option<String>) -> Self {
        self.status = status;
        self
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
    location: Option<String>,
    categories: Vec<String>,
    status: Option<String>,
    source_file: Option<String>,
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
                    let sign = if value.chars().nth(idx) == Some('-') {
                        -1
                    } else {
                        1
                    };
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
            NaiveDate::parse_from_str(clean, "%Y%m%d").map(|d| d.and_hms_opt(0, 0, 0).unwrap())
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

    for ch in duration.chars().skip(1) {
        // Skip 'P'
        match ch {
            'D' => {
                if let Ok(n) = num_str.parse() {
                    days = n;
                    has_unit = true;
                }
                num_str.clear();
            }
            'W' => {
                if let Ok(n) = num_str.parse() {
                    weeks = n;
                    has_unit = true;
                }
                num_str.clear();
            }
            'T' => {
                after_t = true;
                continue;
            }
            'H' if after_t => {
                if let Ok(n) = num_str.parse() {
                    hours = n;
                    has_unit = true;
                }
                num_str.clear();
            }
            'M' if after_t => {
                if let Ok(n) = num_str.parse() {
                    minutes = n;
                    has_unit = true;
                }
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

    Some(
        Duration::days(days)
            + Duration::weeks(weeks)
            + Duration::hours(hours)
            + Duration::minutes(minutes),
    )
}

/// Parse a human-readable duration string like "30m", "1h", "2h30m", "1d"
fn parse_human_duration(s: &str) -> Option<Duration> {
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
                    total += Duration::hours(n);
                }
                current_num.clear();
            }
            'm' | 'M' => {
                if let Ok(n) = current_num.parse() {
                    total += Duration::minutes(n);
                }
                current_num.clear();
            }
            'd' | 'D' => {
                if let Ok(n) = current_num.parse() {
                    total += Duration::days(n);
                }
                current_num.clear();
            }
            'w' | 'W' => {
                if let Ok(n) = current_num.parse() {
                    total += Duration::weeks(n);
                }
                current_num.clear();
            }
            ' ' | '\t' => {}  // ignore whitespace
            _ => return None, // invalid character
        }
    }

    // Handle trailing number without unit (treat as minutes)
    if !current_num.is_empty() {
        if let Ok(n) = current_num.parse() {
            total += Duration::minutes(n);
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

type RRuleParseResult = (
    String,
    NaiveDateTime,
    Option<Vec<String>>,
    Option<i32>,
    Option<i32>,
    Option<Vec<i32>>,
);

fn parse_rrule(rrule: &str) -> Option<RRuleParseResult> {
    let mut freq = None;
    let mut until = None;
    let mut byday = None;
    let mut interval: Option<i32> = None;
    let mut count: Option<i32> = None;
    let mut bymonthday: Option<Vec<i32>> = None;
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
        } else if let Some(v) = part.strip_prefix("BYMONTHDAY=") {
            // BYMONTHDAY can be positive (1-31) or negative (-1 for last day, -2 for second-to-last, etc.)
            bymonthday = Some(v.split(',').filter_map(|d| d.parse().ok()).collect());
        }
    }
    // Use a far-future datetime as default (guaranteed valid since year 2099 is always valid)
    let default_until = NaiveDate::from_ymd_opt(2099, 12, 31)
        .expect("Date 2099-12-31 should always be valid")
        .and_hms_opt(23, 59, 59)
        .expect("Time 23:59:59 should always be valid");
    Some((
        freq?,
        until.unwrap_or(default_until),
        byday,
        interval,
        count,
        bymonthday,
    ))
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
            Some(
                Event::with_recurring(
                    e.summary,
                    e.start,
                    e.end,
                    e.location,
                    e.categories,
                    e.rrule.is_some(),
                )
                .with_source(e.source_file.unwrap_or_default())
                .with_uid(e.uid.clone())
                .with_status(e.status),
            )
        } else {
            None
        }
    }));

    // Expand base events
    for event in base_events {
        let exdate_set: HashSet<NaiveDate> = event.exdates.into_iter().collect();
        let event_uid = event.uid.clone();

        match &event.rrule {
            None => {
                let duration = event.end - event.start;
                if duration.num_minutes() > 0 {
                    result.push(
                        Event::with_recurring(
                            event.summary,
                            event.start,
                            event.end,
                            event.location,
                            event.categories.clone(),
                            false,
                        )
                        .with_source(event.source_file.clone().unwrap_or_default())
                        .with_uid(event_uid)
                        .with_status(event.status.clone()),
                    );
                }
            }
            Some(rrule) => {
                let Some((freq, until, byday, interval, count, bymonthday)) = parse_rrule(rrule)
                else {
                    // Can't parse RRULE, just add the single event
                    let duration = event.end - event.start;
                    if duration.num_minutes() > 0 {
                        result.push(
                            Event::with_recurring(
                                event.summary,
                                event.start,
                                event.end,
                                event.location,
                                event.categories.clone(),
                                true,
                            )
                            .with_source(event.source_file.clone().unwrap_or_default())
                            .with_uid(event_uid.clone())
                            .with_status(event.status.clone()),
                        );
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
                        result.push(
                            Event::with_recurring(
                                event.summary,
                                event.start,
                                event.end,
                                event.location,
                                event.categories.clone(),
                                true,
                            )
                            .with_source(event.source_file.clone().unwrap_or_default())
                            .with_uid(event_uid.clone())
                            .with_status(event.status.clone()),
                        );
                        continue;
                    }
                };

                // Clamp until to avoid unbounded expansion
                let start_date = event.start.date();
                let limit_date = start_date
                    .and_hms_opt(23, 59, 59)
                    .unwrap()
                    .and_utc()
                    .naive_local()
                    + Duration::days(RECURRENCE_LIMIT_DAYS);
                let until = if until > limit_date {
                    limit_date
                } else {
                    until
                };

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
                        result.push(
                            Event::with_recurring(
                                event.summary.clone(),
                                current,
                                current + duration,
                                event.location.clone(),
                                event.categories.clone(),
                                true,
                            )
                            .with_source(event.source_file.clone().unwrap_or_default())
                            .with_uid(event_uid.clone())
                            .with_status(event.status.clone()),
                        );
                    }

                    // Increment to next occurrence
                    if freq == "MONTHLY" {
                        // Handle BYMONTHDAY if specified (e.g., "15th of every month" or "last day")
                        if let Some(ref bmd_list) = bymonthday {
                            let current_year = current.year();
                            let current_month = current.month();

                            // Helper to get valid days for any month
                            let get_valid_days = |year: i32, month: u32| -> Vec<i32> {
                                let days_in_month = NaiveDate::from_ymd_opt(year, month, 1)
                                    .map(|d| d.num_days_in_month() as i32)
                                    .unwrap_or(28);
                                let mut days: Vec<i32> = bmd_list
                                    .iter()
                                    .filter_map(|&day| {
                                        if day > 0 {
                                            if day <= days_in_month {
                                                Some(day)
                                            } else {
                                                None
                                            }
                                        } else {
                                            // Negative: -1 = last day, -2 = second-to-last, etc.
                                            let actual_day = days_in_month + day + 1;
                                            if actual_day > 0 {
                                                Some(actual_day)
                                            } else {
                                                None
                                            }
                                        }
                                    })
                                    .collect();
                                days.sort();
                                days
                            };

                            let valid_days = get_valid_days(current_year, current_month);

                            // Find next valid day in current month
                            let current_day = current.day() as i32;
                            if let Some(&next_day) = valid_days.iter().find(|&&d| d > current_day) {
                                // Use next valid day in current month
                                current = NaiveDate::from_ymd_opt(
                                    current_year,
                                    current_month,
                                    next_day as u32,
                                )
                                .unwrap()
                                .and_hms_opt(current.hour(), current.minute(), current.second())
                                .unwrap_or(current);
                            } else {
                                // Move to first valid day of next month (recalculate for target month)
                                let (next_year, next_month) = if current_month == 12 {
                                    (current_year + 1, 1)
                                } else {
                                    (current_year, current_month + 1)
                                };
                                let next_valid_days = get_valid_days(next_year, next_month);
                                let first_valid = *next_valid_days.first().unwrap_or(&1);
                                current = NaiveDate::from_ymd_opt(
                                    next_year,
                                    next_month,
                                    first_valid as u32,
                                )
                                .unwrap()
                                .and_hms_opt(current.hour(), current.minute(), current.second())
                                .unwrap_or(current);
                            }
                        } else {
                            // Default: increment by one month, keeping same day/time
                            let (year, month) = (current.year(), current.month());
                            let (new_year, new_month) = if month == 12 {
                                (year + 1, 1)
                            } else {
                                (year, month + 1)
                            };
                            // Days in each month (use chrono Datelike trait)
                            let days_in_month_target =
                                NaiveDate::from_ymd_opt(new_year, new_month, 1)
                                    .unwrap()
                                    .num_days_in_month() as u32;
                            // Use original day (clamped to max days in target month)
                            let new_day = original_day.min(days_in_month_target);
                            if let Some(new_date) =
                                NaiveDate::from_ymd_opt(new_year, new_month, new_day)
                            {
                                current = new_date
                                    .and_hms_opt(current.hour(), current.minute(), current.second())
                                    .unwrap_or(current);
                            } else {
                                // Fallback: shouldn't happen with our day calculation
                                current += Duration::days(30);
                            }
                        }
                    } else if freq == "YEARLY" {
                        // Increment by one year
                        let new_year = current.year() + 1;
                        // Clamp original day to valid days in target month
                        let days_in_target_month =
                            NaiveDate::from_ymd_opt(new_year, current.month(), 1)
                                .unwrap()
                                .num_days_in_month() as u32;
                        // Clamp original day to valid days in target month
                        let new_day = original_day.min(days_in_target_month);
                        if let Some(new_date) =
                            NaiveDate::from_ymd_opt(new_year, current.month(), new_day)
                        {
                            current = new_date
                                .and_hms_opt(current.hour(), current.minute(), current.second())
                                .unwrap_or(current);
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

fn extract_raw_events(ical_events: Vec<IcalEvent>, source_file: Option<String>) -> Vec<RawEvent> {
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
            let mut location = None;
            let mut categories = Vec::new();
            let mut status = None;

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
                    "LOCATION" => location = Some(val.to_string()),
                    "CATEGORIES" => {
                        // CATEGORIES can be comma-separated
                        categories = val
                            .split(',')
                            .map(|s| s.trim().to_string())
                            .filter(|s| !s.is_empty())
                            .collect();
                    }
                    "STATUS" => status = Some(val.to_string()),
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
                location,
                categories,
                status,
                source_file: source_file.clone(),
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

    fn write_html(
        &self,
        out: &mut dyn Write,
        quiet: bool,
        sum_only: bool,
        grand_total: i64,
    ) -> io::Result<()> {
        let month_total = self.total_minutes();
        writeln!(out, "  <div class=\"month-section\">")?;
        writeln!(out, "    <h2>📅 {}</h2>", self.month_name)?;
        if !quiet && !sum_only {
            writeln!(out, "    <ul class=\"event-list\">")?;
            for event in &self.events {
                if let Some(mins) = event_duration_minutes(event) {
                    writeln!(out, "      <li class=\"event-item\">")?;
                    writeln!(
                        out,
                        "        <span class=\"summary\">{}</span>",
                        html_escape(&event.summary)
                    )?;
                    writeln!(
                        out,
                        "        <span class=\"duration\">{}</span>",
                        format_hours(mins)
                    )?;
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
            writeln!(
                out,
                "        <span><strong>{}</strong> <span class=\"percentage\">({})</span></span>",
                format_hours(mins),
                pct
            )?;
            writeln!(out, "      </div>")?;
        }
        writeln!(out, "    </div>")?;
        writeln!(out, "    <div class=\"total\">")?;
        writeln!(
            out,
            "      📊 Total: {} ({:.1}%)",
            format_hours(month_total),
            (month_total as f64 / grand_total as f64) * 100.0
        )?;
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
    by_month
        .into_iter()
        .map(|((year, month), evs)| ((year, month), MonthSummary::new(year, month, evs)))
        .collect()
}

/// Groups events by person, sorted alphabetically
fn group_by_person<'a>(events: &'a [&Event]) -> BTreeMap<String, Vec<&'a Event>> {
    let mut by_person: BTreeMap<String, Vec<&'a Event>> = BTreeMap::new();
    for event in events {
        let person = extract_person(&event.summary)
            .unwrap_or("(unknown)")
            .to_string();
        by_person.entry(person).or_default().push(*event);
    }
    by_person
}

/// Groups events by project, sorted alphabetically; events without project go to "(none)"
fn group_by_project<'a>(events: &'a [&Event]) -> BTreeMap<String, Vec<&'a Event>> {
    let mut by_project: BTreeMap<String, Vec<&'a Event>> = BTreeMap::new();
    for event in events {
        let project = extract_project(&event.summary)
            .unwrap_or("(none)")
            .to_string();
        by_project.entry(project).or_default().push(*event);
    }
    by_project
}

/// Groups events by day of week, sorted from Monday to Sunday
fn group_by_weekday<'a>(events: &'a [&Event]) -> BTreeMap<String, Vec<&'a Event>> {
    let weekday_names = [
        "Monday",
        "Tuesday",
        "Wednesday",
        "Thursday",
        "Friday",
        "Saturday",
        "Sunday",
    ];
    let mut by_weekday: BTreeMap<String, Vec<&'a Event>> = BTreeMap::new();
    for event in events {
        let wd = event.start.weekday().num_days_from_monday() as usize;
        let day_name = weekday_names.get(wd).unwrap_or(&"Unknown").to_string();
        by_weekday.entry(day_name).or_default().push(*event);
    }
    by_weekday
}

/// Groups events by location, sorted alphabetically; events without location go to "(none)"
fn group_by_location<'a>(events: &'a [&Event]) -> BTreeMap<String, Vec<&'a Event>> {
    let mut by_location: BTreeMap<String, Vec<&'a Event>> = BTreeMap::new();
    for event in events {
        let location = event
            .location
            .clone()
            .unwrap_or_else(|| "(none)".to_string());
        by_location.entry(location).or_default().push(*event);
    }
    by_location
}

/// Groups events by category, sorted alphabetically; events without category go to "(none)"
fn group_by_category<'a>(events: &'a [&Event]) -> BTreeMap<String, Vec<&'a Event>> {
    let mut by_category: BTreeMap<String, Vec<&'a Event>> = BTreeMap::new();
    for event in events {
        let category = if event.categories.is_empty() {
            "(none)".to_string()
        } else {
            event.categories.join(", ")
        };
        by_category.entry(category).or_default().push(*event);
    }
    by_category
}

/// Groups events by year, sorted chronologically
fn group_by_year<'a>(events: &'a [&Event]) -> BTreeMap<i32, Vec<&'a Event>> {
    let mut by_year: BTreeMap<i32, Vec<&'a Event>> = BTreeMap::new();
    for event in events {
        by_year.entry(event.start.year()).or_default().push(*event);
    }
    by_year
}

fn matches_filter(
    event: &Event,
    filter: &DateFilter,
    now: &NaiveDateTime,
    yesterday: &NaiveDateTime,
    tomorrow: &NaiveDateTime,
) -> bool {
    let (ev_year, ev_month, ev_day) = (event.start.year(), event.start.month(), event.start.day());

    // Compute last week dates internally
    let days_since_monday = now.weekday().num_days_from_monday();
    let last_week_monday =
        now.date() - Duration::weeks(1) - Duration::days(days_since_monday as i64);
    let last_week_sunday = last_week_monday + Duration::days(6);

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
            ev_year == yesterday.year()
                && ev_month == yesterday.month()
                && ev_day == yesterday.day()
        }
        DateFilter::Tomorrow => {
            ev_year == tomorrow.year() && ev_month == tomorrow.month() && ev_day == tomorrow.day()
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
        DateFilter::LastWeek => {
            // Events from last week (Monday to Sunday)
            let ev_date = NaiveDate::from_ymd_opt(ev_year, ev_month, ev_day).unwrap_or_default();
            ev_date >= last_week_monday && ev_date <= last_week_sunday
        }
    }
}

// ISO week number calculation using chrono's built-in support

/// Returns true if event matches the person filter (case-insensitive).
/// Returns true if no filter is set.
fn matches_person_filter(event: &Event, person_filter: &Option<String>) -> bool {
    let Some(filter) = person_filter else {
        return true;
    };
    extract_person(&event.summary)
        .map(|p| p.to_lowercase().contains(&filter.to_lowercase()))
        .unwrap_or(false)
}

/// Returns true if event's person matches ANY of the given person names (OR logic, case-insensitive).
/// Empty list returns true.
fn matches_persons_filter(event: &Event, persons: &[String]) -> bool {
    if persons.is_empty() {
        return true;
    }
    let Some(event_person) = extract_person(&event.summary) else {
        return false;
    };
    let event_person_lower = event_person.to_lowercase();
    persons
        .iter()
        .any(|p| event_person_lower.contains(&p.to_lowercase()))
}

/// Returns true if event matches the project filter (case-insensitive).
/// Returns true if no filter is set.
fn matches_project_filter(event: &Event, project_filter: &Option<String>) -> bool {
    let Some(filter) = project_filter else {
        return true;
    };
    extract_project(&event.summary)
        .map(|p| p.to_lowercase().contains(&filter.to_lowercase()))
        .unwrap_or(false)
}

/// Returns true if event matches a tag filter (matches person OR project, case-insensitive).
/// Returns true if no filter is set.
fn matches_tag_filter(event: &Event, tag_filter: &Option<String>) -> bool {
    let Some(filter) = tag_filter else {
        return true;
    };
    let filter_lower = filter.to_lowercase();
    // Check person
    if extract_person(&event.summary)
        .map(|p| p.to_lowercase().contains(&filter_lower))
        .unwrap_or(false)
    {
        return true;
    }
    // Check project
    extract_project(&event.summary)
        .map(|p| p.to_lowercase().contains(&filter_lower))
        .unwrap_or(false)
}

/// Returns true if event should NOT be excluded based on project exclude filters.
/// Returns true if no exclude filters are set.
fn matches_exclude_project_filter(event: &Event, exclude_filters: &[String]) -> bool {
    let Some(project) = extract_project(&event.summary) else {
        return true;
    };
    let project_lower = project.to_lowercase();
    !exclude_filters
        .iter()
        .any(|f| project_lower.contains(&f.to_lowercase()))
}

/// Returns true if event should NOT be excluded based on person exclude filters.
/// Returns true if no exclude filters are set.
fn matches_exclude_filter(event: &Event, exclude_filters: &[String]) -> bool {
    let Some(person) = extract_person(&event.summary) else {
        return true;
    };
    let person_lower = person.to_lowercase();
    !exclude_filters
        .iter()
        .any(|f| person_lower.contains(&f.to_lowercase()))
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

/// Parse week filter string like "10" or "2024-W10" or "W10-2024"
/// Returns (iso_year, iso_week) if valid, None otherwise
/// iso_year = 0 means no year specified (use current year at match time)
fn parse_week_filter(week_str: &str) -> Option<(i32, u32)> {
    let cleaned = week_str.trim();

    // Try "2024-W10" format
    if cleaned.contains('-') {
        let parts: Vec<&str> = cleaned.split('-').collect();
        if parts.len() == 2 {
            let year: i32 = parts[0].parse().ok()?;
            let week_str = parts[1].trim_start_matches('W').trim_start_matches('w');
            let week: u32 = week_str.parse().ok()?;
            if (1..=53).contains(&week) {
                return Some((year, week));
            }
        }
    }

    // Try "W10" format (current year as sentinel = 0, meaning "any year")
    if let Some(after_w) = cleaned.strip_prefix('W').or(cleaned.strip_prefix('w')) {
        if let Ok(week) = after_w.parse::<u32>() {
            if (1..=53).contains(&week) {
                return Some((0, week)); // 0 = match any year
            }
        }
    }

    // Try bare number "10" (current year as sentinel = 0, meaning "any year")
    if let Ok(week) = cleaned.parse::<u32>() {
        if (1..=53).contains(&week) {
            return Some((0, week)); // 0 = match any year
        }
    }

    None
}

fn matches_week_number_filter(event: &Event, week_filter: &Option<String>) -> bool {
    if let Some(week_str) = week_filter {
        if let Some((filter_year, filter_week)) = parse_week_filter(week_str) {
            let ev_iso = event.start.iso_week();
            // If filter_year is 0, match any year; otherwise match specific year
            let year_matches = filter_year == 0 || ev_iso.year() == filter_year;
            ev_iso.week() == filter_week && year_matches
        } else {
            // Invalid filter string - don't match anything
            false
        }
    } else {
        true
    }
}

/// Returns true if event is within the last N days (including today).
/// Returns true if no recent filter is set.
fn matches_recent_filter(event: &Event, recent_days: &Option<u32>, today: &NaiveDate) -> bool {
    if let Some(days) = recent_days {
        let cutoff = *today - Duration::days(*days as i64 - 1); // inclusive of N days
        event.start.date() >= cutoff && event.start.date() <= *today
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

/// Check if event's weekday matches any of the given weekday abbreviations (case-insensitive).
/// Returns true if weekdays list is empty.
fn matches_weekday_filter(event: &Event, weekdays: &[String]) -> bool {
    if weekdays.is_empty() {
        return true;
    }
    let event_weekday = event.start.weekday().num_days_from_monday() + 1;
    weekdays.iter().any(|day| {
        weekday_abbrev_to_num(day)
            .map(|wd| wd == event_weekday)
            .unwrap_or(false)
    })
}

/// Check if event's weekday matches any of the exclude_weekdays (negation of matches_weekday_filter).
/// Returns true (keep event) if no exclude filters are set.
fn matches_exclude_weekday_filter(event: &Event, exclude_weekdays: &[String]) -> bool {
    if exclude_weekdays.is_empty() {
        return true; // Nothing to exclude
    }
    !matches_weekday_filter(event, exclude_weekdays)
}

fn matches_exclude_summary_filter(event: &Event, exclude_filters: &[String]) -> bool {
    if exclude_filters.is_empty() {
        return true;
    }
    let summary_lower = event.summary.to_lowercase();
    !exclude_filters
        .iter()
        .any(|f| summary_lower.contains(&f.to_lowercase()))
}

/// Returns true if event matches the category filter (case-insensitive).
/// Returns true if no filter is set.
fn matches_category_filter(event: &Event, category_filter: &Option<String>) -> bool {
    let Some(filter) = category_filter else {
        return true;
    };
    event
        .categories
        .iter()
        .any(|c| c.to_lowercase().contains(&filter.to_lowercase()))
}

/// Returns true if event does NOT match any exclude_category filter (case-insensitive).
/// Returns true if no exclude filters are set.
fn matches_exclude_category_filter(event: &Event, exclude_filters: &[String]) -> bool {
    if exclude_filters.is_empty() {
        return true;
    }
    let categories_lower: Vec<String> = event.categories.iter().map(|c| c.to_lowercase()).collect();
    !exclude_filters.iter().any(|f| {
        let f_lower = f.to_lowercase();
        categories_lower.iter().any(|c| c.contains(&f_lower))
    })
}

/// Returns true if event matches the location filter (case-insensitive).
/// Returns true if no filter is set.
fn matches_location_filter(event: &Event, location_filter: &Option<String>) -> bool {
    let Some(filter) = location_filter else {
        return true;
    };
    event
        .location
        .as_ref()
        .map(|l| l.to_lowercase().contains(&filter.to_lowercase()))
        .unwrap_or(false)
}

/// Returns true if event does NOT match any exclude_location filter (case-insensitive).
/// Returns true if no exclude filters are set.
fn matches_exclude_location_filter(event: &Event, exclude_filters: &[String]) -> bool {
    if exclude_filters.is_empty() {
        return true;
    }
    if let Some(ref loc) = event.location {
        let loc_lower = loc.to_lowercase();
        return !exclude_filters
            .iter()
            .any(|f| loc_lower.contains(&f.to_lowercase()));
    }
    true // No location = can't match exclude filter
}

/// Returns true if event matches the status filter (case-insensitive).
/// Returns true if no filter is set.
fn matches_status_filter(event: &Event, status_filter: &Option<EventStatus>) -> bool {
    let Some(filter) = status_filter else {
        return true;
    };
    event
        .status
        .as_ref()
        .map(|s| s.to_lowercase() == filter.to_string().to_lowercase())
        .unwrap_or(false)
}

/// Returns true if event does NOT match any exclude_status filter.
/// Returns true if no exclude filters are set.
fn matches_exclude_status_filter(event: &Event, exclude_filters: &[EventStatus]) -> bool {
    if exclude_filters.is_empty() {
        return true;
    }
    if let Some(ref status) = event.status {
        let status_lower = status.to_lowercase();
        return !exclude_filters
            .iter()
            .any(|f| status_lower == f.to_string().to_lowercase());
    }
    true // No status = can't match exclude filter
}

/// Returns true if event matches ALL search terms (case-insensitive, AND logic).
/// Empty search list matches everything.
fn matches_search_filter(event: &Event, search_terms: &[String]) -> bool {
    if search_terms.is_empty() {
        return true;
    }
    let summary_lower = event.summary.to_lowercase();
    search_terms
        .iter()
        .all(|term| summary_lower.contains(&term.to_lowercase()))
}

/// Returns true if event matches ANY include_summary term (case-insensitive, OR logic).
/// Empty list matches everything.
fn matches_include_summary_filter(event: &Event, include_terms: &[String]) -> bool {
    if include_terms.is_empty() {
        return true;
    }
    let summary_lower = event.summary.to_lowercase();
    include_terms
        .iter()
        .any(|term| summary_lower.contains(&term.to_lowercase()))
}

/// Returns true if event has no [person] tag AND no {project} tag.
/// Returns true if --only-untagged is not set (filter not active).
fn matches_only_untagged_filter(event: &Event, only_untagged: bool) -> bool {
    if only_untagged {
        extract_person(&event.summary).is_none() && extract_project(&event.summary).is_none()
    } else {
        true
    }
}

/// Parse a time string in HH:MM format
fn parse_time(time_str: &str) -> Option<(u32, u32)> {
    let parts: Vec<&str> = time_str.split(':').collect();
    if parts.len() != 2 {
        return None;
    }
    let hours: u32 = parts[0].parse().ok()?;
    let minutes: u32 = parts[1].parse().ok()?;
    if hours > 23 || minutes > 59 {
        return None;
    }
    Some((hours, minutes))
}

/// Returns true if event starts at or after the given time
fn matches_start_after_filter(event: &Event, start_after: &Option<String>) -> bool {
    if let Some(time_str) = start_after {
        if let Some((hours, minutes)) = parse_time(time_str) {
            let event_hours = event.start.hour();
            let event_minutes = event.start.minute();
            if event_hours < hours || (event_hours == hours && event_minutes < minutes) {
                return false;
            }
        }
    }
    true
}

/// Returns true if event starts at or before the given time
fn matches_start_before_filter(event: &Event, start_before: &Option<String>) -> bool {
    if let Some(time_str) = start_before {
        if let Some((hours, minutes)) = parse_time(time_str) {
            let event_hours = event.start.hour();
            let event_minutes = event.start.minute();
            if event_hours > hours || (event_hours == hours && event_minutes > minutes) {
                return false;
            }
        }
    }
    true
}

/// Returns true if event ends at or after the given time
fn matches_end_after_filter(event: &Event, end_after: &Option<String>) -> bool {
    if let Some(time_str) = end_after {
        if let Some((hours, minutes)) = parse_time(time_str) {
            let event_hours = event.end.hour();
            let event_minutes = event.end.minute();
            if event_hours < hours || (event_hours == hours && event_minutes < minutes) {
                return false;
            }
        }
    }
    true
}

/// Returns true if event ends at or before the given time
fn matches_end_before_filter(event: &Event, end_before: &Option<String>) -> bool {
    if let Some(time_str) = end_before {
        if let Some((hours, minutes)) = parse_time(time_str) {
            let event_hours = event.end.hour();
            let event_minutes = event.end.minute();
            if event_hours > hours || (event_hours == hours && event_minutes > minutes) {
                return false;
            }
        }
    }
    true
}

/// Returns true if event should NOT be excluded as recurring.
/// Returns true if --exclude-recurring is not set (filter not active).
/// Returns true for non-recurring events even if filter is active.
fn matches_exclude_recurring_filter(event: &Event, exclude_recurring: bool) -> bool {
    if exclude_recurring {
        !event.is_recurring
    } else {
        true
    }
}

/// Returns true if event is recurring and --include-recurring is set.
/// Returns true if --include-recurring is not set (filter not active).
fn matches_include_recurring_filter(event: &Event, include_recurring: bool) -> bool {
    if include_recurring {
        event.is_recurring
    } else {
        true
    }
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
    uid: Option<String>,
    person: Option<String>,
    project: Option<String>,
    start: String,
    end: String,
    date: String,
    weekday: String,
    duration_minutes: i64,
    duration_formatted: String,
    location: Option<String>,
    categories: Vec<String>,
    status: Option<String>,
    source_file: Option<String>,
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

#[derive(Serialize)]
struct StatsByPerson {
    person: String,
    minutes: i64,
    formatted: String,
    percentage: String,
}

#[derive(Serialize)]
struct StatsByWeekday {
    weekday: String,
    minutes: i64,
    formatted: String,
}

#[derive(Serialize)]
struct StatsOutput {
    total_events: usize,
    total_minutes: i64,
    total_formatted: String,
    date_range_start: Option<String>,
    date_range_end: Option<String>,
    days_span: i64,
    avg_per_day_minutes: i64,
    avg_per_day_formatted: String,
    top_person: Option<String>,
    by_person: Vec<StatsByPerson>,
    by_weekday: Vec<StatsByWeekday>,
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
    if mins > 0 {
        Some(mins)
    } else {
        None
    }
}

/// Escapes a string for CSV output (handles quotes and commas)
fn csv_escape(s: &str) -> String {
    if s.contains('"') || s.contains(',') || s.contains('\n') {
        format!("\"{}\"", s.replace('"', "\"\""))
    } else {
        s.to_string()
    }
}

/// Escapes a string for TOML output (handles quotes and special characters)
fn toml_escape(s: &str) -> String {
    s.replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
        .replace('\r', "\\r")
        .replace('\t', "\\t")
}

/// Returns the appropriate file extension for the given output format
fn get_output_extension(format: &OutputFormat) -> &'static str {
    match format {
        OutputFormat::Text => "txt",
        OutputFormat::Json => "json",
        OutputFormat::Jsonl => "jsonl",
        OutputFormat::Csv => "csv",
        OutputFormat::Markdown => "md",
        OutputFormat::Ical => "ics",
        OutputFormat::Html => "html",
        OutputFormat::Yaml => "yaml",
        OutputFormat::Toml => "toml",
        OutputFormat::Pivot => "txt",
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

/// Helper to build JsonOutput from grouped events
fn build_json_output(
    grouped: &BTreeMap<(i32, u32), MonthSummary>,
    grand_total_minutes: i64,
) -> JsonOutput {
    let mut months_json: Vec<JsonMonthSummary> = Vec::new();
    for ((year, month), summary) in grouped {
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
                    uid: event.uid.clone(),
                    person,
                    project: extract_project(&event.summary).map(|s| s.to_string()),
                    start: event.start.format("%Y-%m-%d %H:%M").to_string(),
                    end: event.end.format("%Y-%m-%d %H:%M").to_string(),
                    date: event.start.format("%Y-%m-%d").to_string(),
                    weekday: event.start.format("%A").to_string(),
                    duration_minutes: mins,
                    duration_formatted: format_hours(mins),
                    location: event.location.clone(),
                    categories: event.categories.clone(),
                    status: event.status.clone(),
                    source_file: event.source_file.clone(),
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

    JsonOutput {
        grand_total_minutes,
        grand_total_formatted: format_hours(grand_total_minutes),
        months: months_json,
    }
}

fn main() -> io::Result<()> {
    let args = Args::parse();

    // Generate shell completion script and exit if requested
    if let Some(shell) = &args.generate_completion {
        let mut cmd = Args::command();
        let name = env!("CARGO_PKG_NAME");
        clap_complete::generate(*shell, &mut cmd, name, &mut std::io::stdout());
        return Ok(());
    }

    // Initialize tracing/logging
    let log_level = if args.verbose {
        Level::DEBUG
    } else {
        Level::INFO
    };
    FmtSubscriber::builder()
        .with_max_level(log_level)
        .with_target(false)
        .with_thread_ids(false)
        .with_file(false)
        .with_line_number(false)
        .compact()
        .init();

    // Apply --no-color flag before any output
    color::set_no_color(args.no_color);

    debug!("proton-extractor v{} starting", VERSION);
    debug!("Processing {} file(s)", args.files.len());
    if let Some(ref p) = args.person {
        debug!("Filtering by person: {}", p);
    }
    if let Some(ref p) = args.project {
        debug!("Filtering by project: {}", p);
    }
    if let Some(ref t) = args.tag {
        debug!("Filtering by tag: {}", t);
    }
    if !args.exclude_person.is_empty() {
        debug!("Excluding persons: {:?}", args.exclude_person);
    }
    if !args.exclude_project.is_empty() {
        debug!("Excluding projects: {:?}", args.exclude_project);
    }
    if !args.exclude_summary.is_empty() {
        debug!("Excluding summaries containing: {:?}", args.exclude_summary);
    }
    if let Some(ref f) = args.from {
        debug!("From date: {}", f);
    }
    if let Some(ref t) = args.to {
        debug!("To date: {}", t);
    }
    if let Some(ref y) = args.year {
        debug!("Filter by year: {}", y);
    }
    if let Some(ref wd) = args.weekdays {
        debug!("Filter by weekdays: {:?}", wd);
    }
    if let Some(ref wd) = args.exclude_weekdays {
        debug!("Exclude weekdays: {:?}", wd);
    }
    if let Some(lim) = args.limit {
        debug!("Limit: {} events", lim);
    }
    if args.today {
        debug!("Quick filter --today: enabled");
    }
    if args.yesterday {
        debug!("Quick filter --yesterday: enabled");
    }
    if args.weekly {
        debug!("Quick filter --weekly: enabled");
    }
    if let Some(days) = args.recent {
        debug!("Quick filter --recent: last {} days", days);
    }

    let has_stdin = args.stdin;
    let has_files = !args.files.is_empty();

    // --validate is a pre-flight check that doesn't require files
    if args.validate {
        let mut has_errors = false;
        let mut validated_count = 0;
        
        // Validate date range
        validated_count += 1;
        if let Err(e) = validate_date_range(&args.from, &args.to) {
            has_errors = true;
            print_error(&e.to_string());
            print_hints(&["--from must be before or equal to --to"][..]);
        }
        
        // Validate month
        validated_count += 1;
        if let Err(e) = validate_month(args.month) {
            has_errors = true;
            print_error(&e.to_string());
            print_hints(&["Month must be 1-12 (e.g., --month 3 for March)"][..]);
        }
        
        // Validate week number
        validated_count += 1;
        if let Err(e) = validate_week_number(&args.week_number) {
            has_errors = true;
            print_error(&e.to_string());
            print_hints(&["Format: W10 (current year) or 2024-W10 (specific year)"][..]);
        }
        
        // Validate weekdays
        validated_count += 2;
        if let Err(e) = validate_weekdays(&args.weekdays, "weekdays") {
            has_errors = true;
            print_error(&e.to_string());
            print_hints(&["Use MO,TU,WE,TH,FR,SA,SU (not full names like 'MONDAY')"][..]);
        }
        if let Err(e) = validate_weekdays(&args.exclude_weekdays, "exclude-weekdays") {
            has_errors = true;
            print_error(&e.to_string());
            print_hints(&["Use MO,TU,WE,TH,FR,SA,SU (not full names like 'MONDAY')"][..]);
        }

        // Validate time filters
        validated_count += 4;
        if let Some(ref t) = args.start_after {
            if let Err(e) = validate_time_filter(t, "start-after") {
                has_errors = true;
                print_error(&e.to_string());
            }
        }
        if let Some(ref t) = args.start_before {
            if let Err(e) = validate_time_filter(t, "start-before") {
                has_errors = true;
                print_error(&e.to_string());
            }
        }
        if let Some(ref t) = args.end_after {
            if let Err(e) = validate_time_filter(t, "end-after") {
                has_errors = true;
                print_error(&e.to_string());
            }
        }
        if let Some(ref t) = args.end_before {
            if let Err(e) = validate_time_filter(t, "end-before") {
                has_errors = true;
                print_error(&e.to_string());
            }
        }

        // Validate duration filters
        validated_count += 2;
        if let Some(ref s) = args.min_duration {
            if parse_human_duration(s).is_none() && parse_duration(s).is_none() {
                has_errors = true;
                print_error(&format!("invalid '{}' for --min-duration", s));
                print_hints(&[
                    "Valid formats: '30m', '1h', '2h30m', '1d', '1w'",
                    "Examples: --min-duration 30m  --min-duration 1h30m"
                ]);
            }
        }
        if let Some(ref s) = args.max_duration {
            if parse_human_duration(s).is_none() && parse_duration(s).is_none() {
                has_errors = true;
                print_error(&format!("invalid '{}' for --max-duration", s));
                print_hints(&[
                    "Valid formats: '30m', '1h', '2h30m', '1d', '1w'",
                    "Examples: --max-duration 4h  --max-duration 8h"
                ]);
            }
        }
        
        if has_errors {
            eprintln!();
            print_error("Validation failed");
            std::process::exit(1);
        }
        
        // Success output
        print_success("All arguments validated successfully");
        println!();
        println!("  {} {} argument constraint(s) checked", colored(color::DIM, "→"), validated_count);
        
        // Show effective filters in verbose mode
        if args.verbose {
            println!();
            println!("  {} Effective filters:", colored(color::CYAN, "→"));
            if args.quiet { println!("    {:<22} quiet mode", colored(color::DIM, "-q,")); }
            if args.silent { println!("    {:<22} silent mode", colored(color::DIM, "--silent")); }
            if args.today { println!("    {:<22} today", colored(color::DIM, "-t,")); }
            if args.yesterday { println!("    {:<22} yesterday", colored(color::DIM, "--yesterday")); }
            if args.weekly { println!("    {:<22} this week", colored(color::DIM, "-w,")); }
            if args.last_week { println!("    {:<22} last week", colored(color::DIM, "-l,")); }
            if !matches!(args.date, DateFilter::All) {
                println!("    {:<22} date filter: {:?}", colored(color::DIM, "-d,"), args.date);
            }
            if let Some(ref p) = args.person {
                println!("    {:<22} person: {}", colored(color::DIM, "--person"), p);
            }
            if let Some(ref p) = args.project {
                println!("    {:<22} project: {}", colored(color::DIM, "--project"), p);
            }
            if let Some(ref t) = args.tag {
                println!("    {:<22} tag: {}", colored(color::DIM, "--tag"), t);
            }
            if let Some(ref f) = args.from {
                println!("    {:<22} from: {}", colored(color::DIM, "--from"), f);
            }
            if let Some(ref t) = args.to {
                println!("    {:<22} to: {}", colored(color::DIM, "--to"), t);
            }
        }
        
        return Ok(());
    }

    // Validate filter arguments early (before checking for files)
    // This provides better error messages for invalid filter values
    if let Err(e) = validate_date_range(&args.from, &args.to) {
        print_error(&e.to_string());
        print_hints(&["--from must be before or equal to --to"][..]);
        std::process::exit(1);
    }
    if let Err(e) = validate_month(args.month) {
        print_error(&e.to_string());
        print_hints(&["Month must be 1-12 (e.g., --month 3 for March)"][..]);
        std::process::exit(1);
    }
    if let Err(e) = validate_week_number(&args.week_number) {
        print_error(&e.to_string());
        print_hints(&["Format: W10 (current year) or 2024-W10 (specific year)"][..]);
        std::process::exit(1);
    }
    if let Err(e) = validate_weekdays(&args.weekdays, "weekdays") {
        print_error(&e.to_string());
        print_hints(&["Use MO,TU,WE,TH,FR,SA,SU (not full names like 'MONDAY')"][..]);
        std::process::exit(1);
    }
    if let Err(e) = validate_weekdays(&args.exclude_weekdays, "exclude-weekdays") {
        print_error(&e.to_string());
        print_hints(&["Use MO,TU,WE,TH,FR,SA,SU (not full names like 'MONDAY')"][..]);
        std::process::exit(1);
    }
    
    // Validate time filters
    if let Some(ref t) = args.start_after {
        if let Err(e) = validate_time_filter(t, "start-after") {
            print_error(&e.to_string());
            print_hints(&["Use HH:MM format (e.g., '09:00' or '17:30')"][..]);
            std::process::exit(1);
        }
    }
    if let Some(ref t) = args.start_before {
        if let Err(e) = validate_time_filter(t, "start-before") {
            print_error(&e.to_string());
            print_hints(&["Use HH:MM format (e.g., '09:00' or '17:30')"][..]);
            std::process::exit(1);
        }
    }
    if let Some(ref t) = args.end_after {
        if let Err(e) = validate_time_filter(t, "end-after") {
            print_error(&e.to_string());
            print_hints(&["Use HH:MM format (e.g., '09:00' or '17:30')"][..]);
            std::process::exit(1);
        }
    }
    if let Some(ref t) = args.end_before {
        if let Err(e) = validate_time_filter(t, "end-before") {
            print_error(&e.to_string());
            print_hints(&["Use HH:MM format (e.g., '09:00' or '17:30')"][..]);
            std::process::exit(1);
        }
    }
    
    // Validate duration filters
    if let Some(ref s) = args.min_duration {
        if parse_human_duration(s).is_none() && parse_duration(s).is_none() {
            print_error(&format!("invalid '{}' for --min-duration", s));
            print_hints(&[
                "Valid formats: '30m', '1h', '2h30m', '1d', '1w'",
                "Examples: --min-duration 30m  --min-duration 1h30m"
            ]);
            std::process::exit(1);
        }
    }
    if let Some(ref s) = args.max_duration {
        if parse_human_duration(s).is_none() && parse_duration(s).is_none() {
            print_error(&format!("invalid '{}' for --max-duration", s));
            print_hints(&[
                "Valid formats: '30m', '1h', '2h30m', '1d', '1w'",
                "Examples: --max-duration 4h  --max-duration 8h"
            ]);
            std::process::exit(1);
        }
    }

    if !has_stdin && !has_files {
        print_error("No .ics files provided");
        print_hints(&[
            "Provide file paths: proton-extractor calendar.ics",
            "Or pipe ICS content: proton-extractor --stdin < calendar.ics",
            "Validate args (CI/CD): proton-extractor --validate [args]",
            "Get help: proton-extractor --help"
        ]);
        std::process::exit(1);
    }

    if has_files && has_stdin {
        print_error("Cannot use both --stdin and file arguments simultaneously");
        print_hints(&["Use either --stdin OR file paths, not both"][..]);
        std::process::exit(1);
    }
    
    let mut all_raw_events = Vec::new();

    if has_stdin {
        // Read from stdin
        debug!("Reading from stdin");
        let reader = BufReader::new(std::io::stdin());
        let mut found_content = false;
        let mut parse_warnings = Vec::new();
        let parser = IcalParser::new(reader);
        for calendar in parser {
            match calendar {
                Ok(cal) => {
                    found_content = true;
                    debug!("Found {} events from stdin", cal.events.len());
                    all_raw_events.extend(extract_raw_events(cal.events, None));
                }
                Err(_e) if args.quiet || args.silent => {}
                Err(e) => {
                    parse_warnings.push(e.to_string());
                }
            }
        }

        // Detect empty stdin early
        if !found_content && !parse_warnings.is_empty() && !args.quiet && !args.silent {
            print_warn("stdin appears to be empty or not valid ICS content");
            print_hints(&["Provide ICS content via pipe: proton-extractor --stdin < calendar.ics"][..]);
        } else if !found_content && !args.quiet && !args.silent && parse_warnings.is_empty() {
            print_warn("stdin is empty");
            print_hints(&["Provide ICS content via pipe: proton-extractor --stdin < calendar.ics"][..]);
        }
    } else {
        // Validate file extensions before processing
        for path in &args.files {
            if let Err(e) = validate_ics_file(path) {
                print_error(&format!("'{}': {}", path.display(), e));
                print_hints(&["Expected: .ics file"][..]);
                std::process::exit(1);
            }
        }

        let is_large_batch = args.files.len() > 1 || args.dry_run;
        let mut spinner = if is_large_batch && !args.quiet && !args.silent {
            Some(Spinner::new("Processing files..."))
        } else {
            None
        };

        for (i, path) in args.files.iter().enumerate() {
            debug!("Reading: {}", path.display());

            // Show progress for multiple files or dry-run
            if let Some(ref mut s) = spinner {
                s.tick();
                if args.files.len() > 1 {
                    eprint!(
                        "\r{} [{}/{}] {}",
                        colored(color::DIM, "→"),
                        i + 1,
                        args.files.len(),
                        colored(color::CYAN, path.display().to_string())
                    );
                } else {
                    eprint!(
                        "\r{} {}",
                        colored(color::DIM, "→"),
                        colored(color::CYAN, path.display().to_string())
                    );
                }
                io::stderr().flush().ok();
            }

            let file = match File::open(path) {
                Ok(f) => f,
                Err(e) => {
                    let path_str = path.display().to_string();
                    let (msg, hints) = match e.kind() {
                        std::io::ErrorKind::NotFound => (
                            format!("'{}' not found", path_str),
                            vec![
                                format!("Verify the path is correct: ls -la {}", path_str),
                                "Check that the file exists and is readable".to_string(),
                            ],
                        ),
                        std::io::ErrorKind::PermissionDenied => (
                            format!("Permission denied: '{}'", path_str),
                            vec![format!("Run: chmod +r {}", path_str)],
                        ),
                        _ => (
                            format!("Failed to open '{}': {}", path_str, e),
                            vec![],
                        ),
                    };
                    // Use spinner error finish if spinner exists, otherwise just print error
                    if let Some(ref s) = spinner {
                        s.finish_with_error(&msg);
                    } else {
                        print_error(&msg);
                    }
                    if !hints.is_empty() {
                        print_hints(&hints.iter().map(|s| s.as_str()).collect::<Vec<_>>());
                    }
                    std::process::exit(1);
                }
            };

            let reader = BufReader::new(file);
            let parser = IcalParser::new(reader);

            for calendar in parser {
                match calendar {
                    Ok(cal) => {
                        if !cal.events.is_empty() {
                            debug!("Found {} events in {}", cal.events.len(), path.display());
                        }
                        let source = path.to_string_lossy().to_string();
                        all_raw_events.extend(extract_raw_events(cal.events, Some(source)));
                    }
                    Err(_e) if args.quiet => {
                        // Suppress parse errors in quiet mode
                    }
                    Err(e) => {
                        print_warn(&format!("Failed to parse '{}': {}", path.display(), e));
                    }
                }
            }
        }

        // Clear the spinner and show completion
        if let Some(ref s) = spinner {
            if args.files.len() > 1 {
                s.finish_with_success(&format!("Processed {} files", args.files.len()));
            } else {
                s.finish();
            }
        }
    }

    debug!("Total raw events: {}", all_raw_events.len());

    let total_raw_events = all_raw_events.len();
    let all_events = expand_events(all_raw_events);

    debug!("Expanded events: {}", all_events.len());
    
    // Show verbose progress for event expansion (recurring events)
    if args.verbose && total_raw_events < all_events.len() {
        eprintln!(
            "{} {} raw events → {} expanded events (including recurring)",
            colored(color::DIM, "→"),
            total_raw_events,
            all_events.len()
        );
    }

    // Remove duplicate events if --dedupe or --dedupe-by-summary is set
    let all_events: Vec<Event> = if args.dedupe || args.dedupe_by_summary {
        let before = all_events.len();
        let deduped: Vec<Event> = if args.dedupe_by_summary {
            // Dedupe by summary only
            let mut seen: BTreeSet<String> = BTreeSet::new();
            all_events
                .into_iter()
                .filter(|e| seen.insert(e.summary.clone()))
                .collect()
        } else {
            // Dedupe by summary + start + end
            let mut unique: BTreeSet<(String, NaiveDateTime, NaiveDateTime)> = BTreeSet::new();
            all_events
                .into_iter()
                .filter(|e| unique.insert((e.summary.clone(), e.start, e.end)))
                .collect()
        };
        let after = deduped.len();
        debug!(
            "Deduplication (by {}): {} events -> {} events (removed {})",
            if args.dedupe_by_summary {
                "summary"
            } else {
                "summary+time"
            },
            before,
            after,
            before - after
        );
        deduped
    } else {
        all_events
    };

    // Parse duration filters with validation
    let min_duration: Option<Duration> = if let Some(ref s) = args.min_duration {
        match parse_human_duration(s).or_else(|| parse_duration(s)) {
            Some(d) => Some(d),
            None => {
                print_error(&format!("invalid '{}' for --min-duration", s));
                print_hints(&[
                    "Valid formats: '30m', '1h', '2h30m', '1d', '1w'",
                    "Examples: --min-duration 30m  --min-duration 1h30m"
                ]);
                std::process::exit(1);
            }
        }
    } else {
        None
    };

    let max_duration: Option<Duration> = if let Some(ref s) = args.max_duration {
        match parse_human_duration(s).or_else(|| parse_duration(s)) {
            Some(d) => Some(d),
            None => {
                print_error(&format!("invalid '{}' for --max-duration", s));
                print_hints(&[
                    "Valid formats: '30m', '1h', '2h30m', '1d', '1w'",
                    "Examples: --max-duration 4h  --max-duration 8h"
                ]);
                std::process::exit(1);
            }
        }
    } else {
        None
    };

    // Validate min < max if both are set
    if let (Some(min), Some(max)) = (&min_duration, &max_duration) {
        if min.num_minutes() > max.num_minutes() {
            let min_str = format!("{}h {}m", min.num_minutes() / 60, min.num_minutes() % 60);
            let max_str = format!("{}h {}m", max.num_minutes() / 60, max.num_minutes() % 60);
            print_error(&format!("--min-duration ({}) must be ≤ --max-duration ({})", min_str, max_str));
            print_hints(&["Ensure --min-duration value is less than or equal to --max-duration"][..]);
            std::process::exit(1);
        }
    }

    if let Some(ref d) = min_duration {
        debug!("Min duration filter: {} minutes", d.num_minutes());
    }
    if let Some(ref d) = max_duration {
        debug!("Max duration filter: {} minutes", d.num_minutes());
    }

    // Create output directory if --output-dir is specified
    if let Some(ref output_dir) = args.output_dir {
        if output_dir.exists() && !args.yes {
            // Check if directory has contents
            match std::fs::read_dir(output_dir) {
                Ok(entries) => {
                    let has_contents = entries.into_iter().any(|e| e.is_ok());
                    if has_contents {
                        // Non-interactive mode: fail safely
                        if !atty::is(atty::Stream::Stdin) {
                            print_error(&format!(
                                "Output directory '{}' is not empty (use --yes or --force to overwrite)",
                                output_dir.display()
                            ));
                            std::process::exit(1);
                        }
                        eprintln!();
                        eprintln!(
                            "{} {}",
                            colored(color::YELLOW, "warning:"),
                            colored(color::BOLD, format!("'{}' is not empty", output_dir.display()))
                        );
                        if !confirm("Continue?") {
                            eprintln!();
                            eprintln!("{} Operation cancelled", colored(color::YELLOW, "○"));
                            eprintln!("  {} Use {} or {} to auto-confirm",
                                colored(color::DIM, "→"),
                                colored(color::CYAN, "--yes"),
                                colored(color::CYAN, "--force"));
                            std::process::exit(1);
                        }
                    }
                }
                Err(_) => {
                    // Can't read dir, proceed anyway
                }
            }
        }
        std::fs::create_dir_all(output_dir).map_err(|e| {
            io::Error::other(format!(
                "Failed to create output directory '{}': {}",
                output_dir.display(),
                e
            ))
        })?;
    }

    // Determine output path
    let output_path: Option<PathBuf> = match (&args.output, &args.output_dir) {
        (Some(path), _) => Some(path.clone()),
        (None, Some(dir)) => {
            // Use the first input file's stem as the output filename
            let filename = if let Some(first_input) = args.files.first() {
                let stem = first_input
                    .file_stem()
                    .and_then(|s| s.to_str())
                    .unwrap_or("output");
                format!("{}.{}", stem, get_output_extension(&args.format))
            } else {
                format!("output.{}", get_output_extension(&args.format))
            };
            Some(dir.join(filename))
        }
        (None, None) => None,
    };

    // Setup output: file or stdout
    let mut write_to_file = false;
    let mut output_file_path: Option<PathBuf> = None;
    let out_writer: Box<dyn Write> = match &output_path {
        Some(path) => {
            write_to_file = true;
            output_file_path = Some(path.clone());
            // Check if file exists and prompt for confirmation unless --force or --yes is set
            if path.exists() && !args.yes {
                // Non-interactive mode: fail safely instead of hanging
                if !atty::is(atty::Stream::Stdin) {
                    print_error(&format!(
                        "Output file '{}' already exists (use --yes or --force to overwrite)",
                        path.display()
                    ));
                    std::process::exit(1);
                }
                eprintln!();
                eprintln!(
                    "{} {}",
                    colored(color::YELLOW, "warning:"),
                    colored(color::BOLD, format!("'{}' already exists", path.display()))
                );
                if !confirm("Overwrite?") {
                    eprintln!();
                    eprintln!("{} Operation cancelled", colored(color::YELLOW, "○"));
                    eprintln!("  {} Use {} or {} to auto-confirm",
                        colored(color::DIM, "→"),
                        colored(color::CYAN, "--yes"),
                        colored(color::CYAN, "--force"));
                    std::process::exit(1);
                }
            }
            let file = File::create(path).map_err(|e| {
                io::Error::new(
                    io::ErrorKind::Other,
                    format!("Failed to create output file '{}': {}", path.display(), e),
                )
            })?;
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
    } else if args.tomorrow {
        DateFilter::Tomorrow
    } else if args.weekly {
        DateFilter::Week
    } else if args.last_week {
        DateFilter::LastWeek
    } else {
        args.date.clone()
    };

    // Compute current date once for date filters (avoid calling Local::now() per event)
    let now = Local::now().naive_local();
    let yesterday = now - Duration::days(1);
    let tomorrow = now + Duration::days(1);

    let filtered: Vec<&Event> = all_events
        .iter()
        .filter(|e| matches_filter(e, &effective_date, &now, &yesterday, &tomorrow))
        .filter(|e| matches_person_filter(e, &args.person))
        .filter(|e| matches_persons_filter(e, &args.persons.clone().unwrap_or_default()))
        .filter(|e| matches_project_filter(e, &args.project))
        .filter(|e| matches_tag_filter(e, &args.tag))
        .filter(|e| matches_exclude_filter(e, &args.exclude_person))
        .filter(|e| matches_exclude_project_filter(e, &args.exclude_project))
        .filter(|e| matches_exclude_summary_filter(e, &args.exclude_summary))
        .filter(|e| matches_search_filter(e, &args.search))
        .filter(|e| matches_date_range(e, &args.from, &args.to))
        .filter(|e| matches_year_filter(e, &args.year))
        .filter(|e| matches_month_filter(e, &args.month))
        .filter(|e| matches_week_number_filter(e, &args.week_number))
        .filter(|e| matches_weekday_filter(e, &weekdays_filter))
        .filter(|e| matches_exclude_weekday_filter(e, &exclude_weekdays_filter))
        .filter(|e| matches_category_filter(e, &args.category))
        .filter(|e| matches_exclude_category_filter(e, &args.exclude_category))
        .filter(|e| matches_location_filter(e, &args.location))
        .filter(|e| matches_exclude_location_filter(e, &args.exclude_location))
        .filter(|e| matches_duration_filter(e, &min_duration, &max_duration))
        .filter(|e| matches_exclude_recurring_filter(e, args.exclude_recurring))
        .filter(|e| matches_include_recurring_filter(e, args.include_recurring))
        .filter(|e| matches_status_filter(e, &args.status))
        .filter(|e| matches_exclude_status_filter(e, &args.exclude_status))
        .filter(|e| matches_recent_filter(e, &args.recent, &now.date()))
        .filter(|e| matches_start_after_filter(e, &args.start_after))
        .filter(|e| matches_start_before_filter(e, &args.start_before))
        .filter(|e| matches_end_after_filter(e, &args.end_after))
        .filter(|e| matches_end_before_filter(e, &args.end_before))
        .filter(|e| matches_include_summary_filter(e, &args.include_summary))
        .filter(|e| matches_only_untagged_filter(e, args.only_untagged))
        .take(args.limit.unwrap_or(usize::MAX))
        .collect();

    debug!("Events after filtering: {}", filtered.len());

    // Apply sorting based on --sort-by and --sort-reverse flags
    let mut filtered: Vec<&Event> = filtered;
    match args.sort_by {
        SortBy::Date | SortBy::Start => {
            filtered.sort_by(|a, b| {
                let cmp = a.start.cmp(&b.start);
                if args.sort_reverse {
                    cmp.reverse()
                } else {
                    cmp
                }
            });
        }
        SortBy::End => {
            filtered.sort_by(|a, b| {
                let cmp = a.end.cmp(&b.end);
                if args.sort_reverse {
                    cmp.reverse()
                } else {
                    cmp
                }
            });
        }
        SortBy::Duration => {
            filtered.sort_by(|a, b| {
                let dur_a = event_duration_minutes(a).unwrap_or(0);
                let dur_b = event_duration_minutes(b).unwrap_or(0);
                let cmp = dur_a.cmp(&dur_b);
                if args.sort_reverse {
                    cmp.reverse()
                } else {
                    cmp
                }
            });
        }
        SortBy::Person => {
            filtered.sort_by(|a, b| {
                let pers_a = extract_person(&a.summary).unwrap_or("(unknown)");
                let pers_b = extract_person(&b.summary).unwrap_or("(unknown)");
                let cmp = pers_a.to_lowercase().cmp(&pers_b.to_lowercase());
                if args.sort_reverse {
                    cmp.reverse()
                } else {
                    cmp
                }
            });
        }
        SortBy::Project => {
            filtered.sort_by(|a, b| {
                let proj_a = extract_project(&a.summary).unwrap_or("(none)");
                let proj_b = extract_project(&b.summary).unwrap_or("(none)");
                let cmp = proj_a.to_lowercase().cmp(&proj_b.to_lowercase());
                if args.sort_reverse {
                    cmp.reverse()
                } else {
                    cmp
                }
            });
        }
    }

    if filtered.is_empty() {
        if !args.quiet && !args.silent {
            eprintln!();
            eprintln!("{}  {}", colored(color::YELLOW, "╭─"), colored(color::DIM, "─".repeat(30)));
            eprintln!("{}  {}", colored(color::YELLOW, "│"), colored(color::CYAN, "no events found"));
            eprintln!("{}  {}", colored(color::YELLOW, "╰─"), colored(color::DIM, "─".repeat(30)));
            eprintln!();
            eprintln!("  {} Use {} to debug argument issues", colored(color::DIM, "→"), colored(color::CYAN, "proton-extractor --validate [your args]"));
            eprintln!("  {} Quick check: {} (show all events)", colored(color::DIM, "→"), colored(color::CYAN, "proton-extractor [file] -d all"));
        }

        // Show active date context if a date filter is active
        if !args.quiet && !args.silent {
            match &effective_date {
                DateFilter::Today => eprintln!("  {}  Showing events for today ({})", colored(color::DIM, "→"), now.format("%Y-%m-%d")),
                DateFilter::Yesterday => eprintln!("  {}  Showing events for yesterday ({})", colored(color::DIM, "→"), yesterday.format("%Y-%m-%d")),
                DateFilter::Tomorrow => eprintln!("  {}  Showing events for tomorrow ({})", colored(color::DIM, "→"), tomorrow.format("%Y-%m-%d")),
                DateFilter::Week => eprintln!("  {}  Showing events for ISO week {} ({} to {})", colored(color::DIM, "→"), now.format("%V"), now.date().year().to_string() + "-W" + &now.format("%V").to_string(), tomorrow.format("%Y-%m-%d")),
                DateFilter::LastWeek => eprintln!("  {}  Showing events for last week", colored(color::DIM, "→")),
                DateFilter::Current => eprintln!("  {}  Showing events for {} {}", colored(color::DIM, "→"), now.format("%B"), now.year()),
                DateFilter::Previous => {
                    let (y, m) = if now.month() == 1 { (now.year() - 1, 12) } else { (now.year(), now.month() - 1) };
                    eprintln!("  {}  Showing events for {} {}", colored(color::DIM, "→"), chrono::Month::try_from(u8::try_from(m).unwrap_or(1)).unwrap_or(chrono::Month::January).name(), y);
                },
                DateFilter::All => {}
            }

            // Show date range if --from/--to is set
            if let (Some(from), Some(to)) = (&args.from, &args.to) {
                eprintln!("  {}  Date range: {} to {}", colored(color::DIM, "→"), from, to);
            }

            eprintln!();
            eprintln!("  {} Try these options:", colored(color::CYAN, "→"));

            // Context-aware suggestions based on active filters
            if args.today || args.yesterday || args.tomorrow || args.weekly || args.last_week {
                eprintln!("    {} {:<22} Show all events (no date filter)", colored(color::DIM, "•"), colored(color::CYAN, "-d all"));
            }
            if args.person.is_some() || !args.persons.clone().unwrap_or_default().is_empty() {
                eprintln!("    {} {:<22} Remove person filter", colored(color::DIM, "•"), colored(color::CYAN, "--person ''"));
            }
            if args.project.is_some() {
                eprintln!("    {} {:<22} Remove project filter", colored(color::DIM, "•"), colored(color::CYAN, "--project ''"));
            }
            if args.exclude_recurring {
                eprintln!("    {} {:<22} Include recurring events", colored(color::DIM, "•"), colored(color::CYAN, "--include-recurring"));
            }
            if args.only_untagged {
                eprintln!("    {} {:<22} Include tagged events", colored(color::DIM, "•"), colored(color::CYAN, "--only-untagged=false"));
            }
            if args.from.is_none() && args.to.is_none() {
                eprintln!("    {} {:<22} Filter by date range", colored(color::DIM, "•"), colored(color::CYAN, "--from YYYY-MM-DD --to YYYY-MM-DD"));
            }
            if args.verbose {
                eprintln!("    {} {:<22} Disable verbose mode", colored(color::DIM, "•"), colored(color::CYAN, "-v"));
            } else {
                eprintln!("    {} {:<22} Show debug info", colored(color::DIM, "•"), colored(color::CYAN, "-v"));
            }
            if args.recent.is_some() {
                eprintln!("    {} {:<22} Show events from last N days (no limit)", colored(color::DIM, "•"), colored(color::CYAN, "--recent 30"));
            }

            eprintln!();
            eprintln!("  {} Run {} for all filter options",
                colored(color::DIM, "→"),
                colored(color::CYAN, "proton-extractor --help"));
        }
        return Ok(());
    }

    let grouped: BTreeMap<(i32, u32), MonthSummary> = group_by_month(&filtered);

    // No events found - show helpful context and suggestions
    if grouped.is_empty() {
        let total_raw = total_raw_events;

        if total_raw == 0 {
            // No events in files at all
            print_warn("No calendar events found in the input files");
            eprintln!("  {} Check that your .ics files contain valid VEVENT components", colored(color::DIM, "→"));
        } else {
            // Events exist but were filtered out
            print_notice(&format!("No events match your filters ({} events in {} files)",
                colored(color::YELLOW, total_raw.to_string()),
                colored(color::YELLOW, args.files.len().to_string())));
            eprintln!();
            eprintln!("  {} Suggestions:", colored(color::CYAN, "→"));
            eprintln!("    {} {:<28} Show all events", colored(color::DIM, "•"), colored(color::CYAN, "-d all"));
            if args.person.is_none() && args.project.is_none() && args.tag.is_none() {
                eprintln!("    {} {:<28} List available persons", colored(color::DIM, "•"), colored(color::CYAN, "-P"));
            }
            eprintln!("    {} {:<28} Last 30 days", colored(color::DIM, "•"), colored(color::CYAN, "--recent 30"));
            eprintln!("    {} {:<28} Include recurring", colored(color::DIM, "•"), colored(color::CYAN, "--include-recurring"));
        }
        eprintln!();
        eprintln!("  {} Run {} for filter options",
            colored(color::DIM, "→"),
            colored(color::CYAN, "proton-extractor --help"));
        return Ok(());
    }

    // Dry run mode: just show event count
    if args.dry_run {
        let mut by_person: BTreeMap<&str, usize> = BTreeMap::new();
        for event in &filtered {
            let person = extract_person(&event.summary).unwrap_or("(unknown)");
            *by_person.entry(person).or_default() += 1;
        }
        println!("{}", colored(color::CYAN, "━━━ Dry Run Results ━━━"));
        println!(
            "Total events: {}",
            colored(color::YELLOW, filtered.len().to_string())
        );
        if !by_person.is_empty() {
            println!("\n{}", colored(color::CYAN, "By person:"));
            for (person, count) in &by_person {
                println!(
                    "  {}: {}",
                    person,
                    colored(color::YELLOW, count.to_string())
                );
            }
        }
        if let Some(first) = filtered.first() {
            if let Some(last) = filtered.last() {
                println!(
                    "\nDate range: {} to {}",
                    first.start.format("%Y-%m-%d"),
                    last.start.format("%Y-%m-%d")
                );
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
        let count = sorted.len();
        for person in sorted {
            writeln!(out_writer, "{}", person)?;
        }
        if count > 0 {
            print_list_summary(count, "person");
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
        let count = sorted.len();
        for project in sorted {
            writeln!(out_writer, "{}", project)?;
        }
        if count > 0 {
            print_list_summary(count, "project");
        }
        return Ok(());
    }

    // Collect all unique locations if --list-locations is requested
    if args.list_locations {
        let mut locations: HashSet<String> = HashSet::new();
        for event in &filtered {
            if let Some(ref loc) = event.location {
                if !loc.is_empty() {
                    locations.insert(loc.clone());
                }
            }
        }
        let mut sorted: Vec<_> = locations.into_iter().collect();
        sorted.sort();
        let count = sorted.len();
        for location in sorted {
            writeln!(out_writer, "{}", location)?;
        }
        if count > 0 {
            print_list_summary(count, "location");
        }
        return Ok(());
    }

    // Collect all unique categories if --list-categories is requested
    if args.list_categories {
        let mut categories: HashSet<String> = HashSet::new();
        for event in &filtered {
            for cat in &event.categories {
                if !cat.is_empty() {
                    categories.insert(cat.clone());
                }
            }
        }
        let mut sorted: Vec<_> = categories.into_iter().collect();
        sorted.sort();
        let count = sorted.len();
        for category in sorted {
            writeln!(out_writer, "{}", category)?;
        }
        if count > 0 {
            print_list_summary(count, "category");
        }
        return Ok(());
    }

    // List all unique tags if --list-tags is requested
    if args.list_tags {
        let mut persons: HashSet<String> = HashSet::new();
        let mut projects: HashSet<String> = HashSet::new();
        for event in &filtered {
            if let Some(p) = extract_person(&event.summary) {
                persons.insert(p.to_string());
            }
            if let Some(p) = extract_project(&event.summary) {
                projects.insert(p.to_string());
            }
        }
        let mut sorted_persons: Vec<_> = persons.into_iter().collect();
        let mut sorted_projects: Vec<_> = projects.into_iter().collect();
        sorted_persons.sort();
        sorted_projects.sort();

        if !sorted_persons.is_empty() {
            writeln!(out_writer, "{}", colored(color::CYAN, "Persons:"))?;
            for person in &sorted_persons {
                writeln!(out_writer, "  [{}]", person)?;
            }
        }
        if !sorted_projects.is_empty() {
            writeln!(out_writer, "{}", colored(color::CYAN, "Projects:"))?;
            for project in &sorted_projects {
                writeln!(out_writer, "  {{{}}}", project)?;
            }
        }
        let person_count = sorted_persons.len();
        let project_count = sorted_projects.len();
        let total = person_count + project_count;
        if total > 0 {
            eprintln!(
                "{} {} {} found {}",
                colored(color::GREEN, "✓"),
                colored(color::BOLD, format!("{} tag{}", total, if total == 1 {""} else {"s"})),
                colored(color::DIM, "found"),
                colored(color::DIM, format!("({} persons, {} projects)", person_count, project_count))
            );
        }
        return Ok(());
    }

    // List all unique years if --list-years is requested
    if args.list_years {
        let mut years: BTreeSet<i32> = BTreeSet::new();
        for event in &filtered {
            years.insert(event.start.year());
        }
        let count = years.len();
        for year in years {
            writeln!(out_writer, "{}", year)?;
        }
        if count > 0 {
            print_list_summary(count, "year");
        }
        return Ok(());
    }

    // List all unique UIDs if --list-uids is requested
    if args.list_uids {
        let mut uids: BTreeSet<String> = BTreeSet::new();
        for event in &filtered {
            if let Some(ref uid) = event.uid {
                if !uid.is_empty() {
                    uids.insert(uid.clone());
                }
            }
        }
        let count = uids.len();
        for uid in uids {
            writeln!(out_writer, "{}", uid)?;
        }
        if count > 0 {
            print_list_summary(count, "UID");
        }
        return Ok(());
    }

    // List all unique events if --list-events is requested
    if args.list_events {
        let count = filtered.len();
        for event in &filtered {
            writeln!(
                out_writer,
                "{} | {} | {}",
                event.start.format("%Y-%m-%d %H:%M"),
                event.summary,
                format_hours(event_duration_minutes(event).unwrap_or(0))
            )?;
        }
        if count > 0 {
            print_list_summary(count, "event");
        }
        return Ok(());
    }

    let grand_total_minutes: i64 = filtered
        .iter()
        .filter_map(|e| event_duration_minutes(e))
        .sum();

    // Total only mode: show just the grand total (useful for scripting)
    if args.total_only {
        writeln!(out_writer, "{}", format_hours(grand_total_minutes))?;
        return Ok(());
    }

    // Show top N events by duration if --top is requested
    if let Some(top_n) = args.top {
        let mut events_with_duration: Vec<_> = filtered
            .iter()
            .filter_map(|e| event_duration_minutes(e).map(|mins| (mins, e)))
            .collect();

        events_with_duration.sort_by(|a, b| b.0.cmp(&a.0));

        let top_count = top_n.min(events_with_duration.len());
        let top_total: i64 = events_with_duration
            .iter()
            .take(top_count)
            .map(|(m, _)| m)
            .sum();

        writeln!(
            out_writer,
            "{}",
            colored(
                color::CYAN,
                format!("Top {} events by duration:", top_count)
            )
        )?;
        writeln!(out_writer, "{}", colored(color::CYAN, "=".repeat(40)))?;
        for (mins, event) in events_with_duration.iter().take(top_count) {
            writeln!(
                out_writer,
                "  {}  {}  {}",
                colored(color::YELLOW, format_hours(*mins)),
                event.start.format("%Y-%m-%d"),
                event.summary
            )?;
        }
        writeln!(out_writer)?;
        writeln!(
            out_writer,
            "  {}  {}",
            colored(color::GREEN, format_hours(top_total)),
            colored(color::BOLD, "Top events total")
        )?;
        return Ok(());
    }

    // Show bottom N events by duration if --bottom is requested
    if let Some(bottom_n) = args.bottom {
        let mut events_with_duration: Vec<_> = filtered
            .iter()
            .filter_map(|e| event_duration_minutes(e).map(|mins| (mins, e)))
            .collect();

        events_with_duration.sort_by(|a, b| a.0.cmp(&b.0));

        let bottom_count = bottom_n.min(events_with_duration.len());
        let bottom_total: i64 = events_with_duration
            .iter()
            .take(bottom_count)
            .map(|(m, _)| m)
            .sum();

        writeln!(
            out_writer,
            "{}",
            colored(
                color::CYAN,
                format!("Bottom {} events by duration:", bottom_count)
            )
        )?;
        writeln!(out_writer, "{}", colored(color::CYAN, "=".repeat(40)))?;
        for (mins, event) in events_with_duration.iter().take(bottom_count) {
            writeln!(
                out_writer,
                "  {}  {}  {}",
                colored(color::YELLOW, format_hours(*mins)),
                event.start.format("%Y-%m-%d"),
                event.summary
            )?;
        }
        writeln!(out_writer)?;
        writeln!(
            out_writer,
            "  {}  {}",
            colored(color::GREEN, format_hours(bottom_total)),
            colored(color::BOLD, "Bottom events total")
        )?;
        return Ok(());
    }

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
        let weekday_names = [
            "Monday",
            "Tuesday",
            "Wednesday",
            "Thursday",
            "Friday",
            "Saturday",
            "Sunday",
        ];
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
        let (days_span, avg_per_day) = if let (Some(min_d), Some(max_d)) = (min_date, max_date) {
            let span = (*max_d - *min_d).num_days() + 1;
            (
                span,
                if span > 0 {
                    total_mins / span
                } else {
                    total_mins
                },
            )
        } else {
            (0, 0)
        };

        // Top person
        let top_person_entry = by_person.iter().max_by_key(|(_, v)| *v);

        match args.stats_format {
            StatsFormat::Text => {
                writeln!(out_writer, "📊 Statistics")?;
                writeln!(out_writer, "{}", colored(color::CYAN, "============"))?;
                writeln!(out_writer)?;
                writeln!(
                    out_writer,
                    "Total events:  {}",
                    colored(color::YELLOW, total_events.to_string())
                )?;
                writeln!(
                    out_writer,
                    "Total hours:    {}",
                    colored(color::YELLOW, format_hours(total_mins))
                )?;
                if let (Some(min_d), Some(max_d)) = (min_date, max_date) {
                    writeln!(
                        out_writer,
                        "Date range:     {} to {} ({} days)",
                        min_d, max_d, days_span
                    )?;
                    writeln!(
                        out_writer,
                        "Avg per day:    {}",
                        colored(color::YELLOW, format_hours(avg_per_day))
                    )?;
                }

                writeln!(out_writer)?;
                writeln!(out_writer, "{}", colored(color::CYAN, "By Person"))?;
                writeln!(out_writer, "{}", colored(color::CYAN, "--------"))?;
                if !by_person.is_empty() {
                    for (person, mins) in &by_person {
                        let marker = if Some((person, mins)) == top_person_entry {
                            " 🏆"
                        } else {
                            ""
                        };
                        let pct = format_percentage(*mins, total_mins);
                        writeln!(
                            out_writer,
                            "  {}  {:>6}  ({}){}",
                            colored(color::YELLOW, format_hours(*mins)),
                            colored(color::MAGENTA, pct),
                            person,
                            marker
                        )?;
                    }
                } else {
                    writeln!(out_writer, "  (no person data)")?;
                }

                writeln!(out_writer)?;
                writeln!(out_writer, "{}", colored(color::CYAN, "By Weekday"))?;
                writeln!(out_writer, "{}", colored(color::CYAN, "------------"))?;
                for (day, mins) in &by_weekday {
                    writeln!(
                        out_writer,
                        "  {}  {}",
                        colored(color::YELLOW, format_hours(*mins)),
                        day
                    )?;
                }
            }
            StatsFormat::Json | StatsFormat::Yaml => {
                let stats = StatsOutput {
                    total_events,
                    total_minutes: total_mins,
                    total_formatted: format_hours(total_mins),
                    date_range_start: min_date.map(|d| d.format("%Y-%m-%d").to_string()),
                    date_range_end: max_date.map(|d| d.format("%Y-%m-%d").to_string()),
                    days_span,
                    avg_per_day_minutes: avg_per_day,
                    avg_per_day_formatted: format_hours(avg_per_day),
                    top_person: top_person_entry.map(|(p, _)| p.to_string()),
                    by_person: by_person
                        .iter()
                        .map(|(person, mins)| StatsByPerson {
                            person: person.to_string(),
                            minutes: *mins,
                            formatted: format_hours(*mins),
                            percentage: format_percentage(*mins, total_mins),
                        })
                        .collect(),
                    by_weekday: by_weekday
                        .iter()
                        .map(|(day, mins)| StatsByWeekday {
                            weekday: day.to_string(),
                            minutes: *mins,
                            formatted: format_hours(*mins),
                        })
                        .collect(),
                };

                match args.stats_format {
                    StatsFormat::Json => {
                        let json_str = serde_json::to_string_pretty(&stats)
                            .unwrap_or_else(|_| "{}".to_string());
                        writeln!(out_writer, "{}", json_str)?;
                    }
                    StatsFormat::Yaml => {
                        let yaml_str =
                            serde_yaml::to_string(&stats).unwrap_or_else(|_| "{}".to_string());
                        writeln!(out_writer, "{}", yaml_str)?;
                    }
                    StatsFormat::Text => unreachable!(),
                }
            }
        }

        // Show export confirmation for file outputs
        if write_to_file {
            if let Some(ref path) = output_file_path {
                let count = filtered.len();
                print_exported(count, path);
            }
        }

        return Ok(());
    }

    // Handle --json / -j flag
    let effective_format = if args.json_format {
        OutputFormat::Json
    } else {
        args.format.clone()
    };

    match effective_format {
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
                wtr.write_record([
                    "date",
                    "start",
                    "end",
                    "duration_minutes",
                    "weekday",
                    "person",
                    "project",
                    "summary",
                    "location",
                    "categories",
                ])
                .ok();
                for event in &filtered {
                    let mins = match event_duration_minutes(event) {
                        Some(m) => m,
                        None => continue,
                    };
                    let person = extract_person(&event.summary).unwrap_or("(unknown)");
                    let project = extract_project(&event.summary).unwrap_or("");
                    wtr.write_record(&[
                        event.start.format("%Y-%m-%d").to_string(),
                        event.start.format("%H:%M").to_string(),
                        event.end.format("%H:%M").to_string(),
                        mins.to_string(),
                        event.start.format("%a").to_string(),
                        csv_escape(person),
                        csv_escape(project),
                        csv_escape(&event.summary),
                        csv_escape(event.location.as_deref().unwrap_or("")),
                        csv_escape(&event.categories.join(", ")),
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
                    "".to_string(),
                    "".to_string(),
                    "".to_string(),
                    "".to_string(),
                ])
                .ok();
            }
            wtr.flush().ok();
        }
        OutputFormat::Json => {
            let json_output = build_json_output(&grouped, grand_total_minutes);
            let json_str = if args.compact {
                serde_json::to_string(&json_output).unwrap_or_else(|_| "{}".to_string())
            } else {
                serde_json::to_string_pretty(&json_output).unwrap_or_else(|_| "{}".to_string())
            };
            writeln!(out_writer, "{}", json_str)?;
        }
        OutputFormat::Jsonl => {
            // JSON Lines format - one event per line, ideal for jq/stream processing
            for event in &filtered {
                if let Some(mins) = event_duration_minutes(event) {
                    let person = extract_person(&event.summary).map(|s| s.to_string());
                    let project = extract_project(&event.summary).map(|s| s.to_string());
                    let json_event = serde_json::json!({
                        "summary": event.summary,
                        "uid": event.uid,
                        "person": person,
                        "project": project,
                        "start": event.start.format("%Y-%m-%d %H:%M").to_string(),
                        "end": event.end.format("%Y-%m-%d %H:%M").to_string(),
                        "date": event.start.format("%Y-%m-%d").to_string(),
                        "weekday": event.start.format("%A").to_string(),
                        "duration_minutes": mins,
                        "duration_formatted": format_hours(mins),
                        "location": event.location,
                        "categories": event.categories,
                        "status": event.status,
                        "source_file": event.source_file,
                    });
                    writeln!(out_writer, "{}", json_event)?;
                }
            }
        }
        OutputFormat::Ical => {
            writeln!(out_writer, "BEGIN:VCALENDAR")?;
            writeln!(out_writer, "VERSION:2.0")?;
            writeln!(out_writer, "PRODID:-//proton-extractor//EN")?;
            for event in &filtered {
                writeln!(out_writer, "BEGIN:VEVENT")?;
                // Use original UID if available, otherwise generate one
                let uid = event.uid.clone().unwrap_or_else(|| {
                    format!("{}@proton-extractor", event.start.and_utc().timestamp())
                });
                writeln!(out_writer, "UID:{}", uid)?;
                writeln!(
                    out_writer,
                    "DTSTAMP:{}",
                    event.start.format("%Y%m%dT%H%M%S")
                )?;
                writeln!(
                    out_writer,
                    "DTSTART:{}",
                    event.start.format("%Y%m%dT%H%M%S")
                )?;
                writeln!(out_writer, "DTEND:{}", event.end.format("%Y%m%dT%H%M%S"))?;
                // Escape summary for iCal format
                let summary_escaped = event
                    .summary
                    .replace("\\", "\\\\")
                    .replace(";", "\\;")
                    .replace(",", "\\,")
                    .replace("\n", "\\n");
                writeln!(out_writer, "SUMMARY:{}", summary_escaped)?;
                // Add location if available
                if let Some(ref loc) = event.location {
                    if !loc.is_empty() {
                        let loc_escaped = loc
                            .replace("\\", "\\\\")
                            .replace(";", "\\;")
                            .replace(",", "\\,")
                            .replace("\n", "\\n");
                        writeln!(out_writer, "LOCATION:{}", loc_escaped)?;
                    }
                }
                // Add categories if available
                if !event.categories.is_empty() {
                    let cats_escaped = event
                        .categories
                        .join(",")
                        .replace("\\", "\\\\")
                        .replace(";", "\\;")
                        .replace(",", "\\,")
                        .replace("\n", "\\n");
                    writeln!(out_writer, "CATEGORIES:{}", cats_escaped)?;
                }
                // Add status if available
                if let Some(ref status) = event.status {
                    if !status.is_empty() {
                        writeln!(out_writer, "STATUS:{}", status)?;
                    }
                }
                writeln!(out_writer, "END:VEVENT")?;
            }
            writeln!(out_writer, "END:VCALENDAR")?;
        }
        OutputFormat::Yaml => {
            let json_output = build_json_output(&grouped, grand_total_minutes);
            let yaml_output =
                serde_yaml::to_string(&json_output).unwrap_or_else(|_| "{}".to_string());
            writeln!(out_writer, "{}", yaml_output)?;
        }
        OutputFormat::Toml => {
            // Build TOML-friendly structure with proper nested arrays of tables
            let mut toml_output = String::new();
            toml_output.push_str(&format!("grand_total_minutes = {}\n", grand_total_minutes));
            toml_output.push_str(&format!(
                "grand_total_formatted = \"{}\"\n\n",
                format_hours(grand_total_minutes)
            ));

            for ((year, month), summary) in &grouped {
                let month_total = summary.total_minutes();
                toml_output.push_str("[[months]]\n");
                toml_output.push_str("[months.meta]\n");
                toml_output.push_str(&format!("year = {}\n", year));
                toml_output.push_str(&format!("month = {}\n", month));
                toml_output.push_str(&format!("month_name = \"{}\"\n", summary.month_name));
                toml_output.push_str(&format!("total_minutes = {}\n", month_total));
                toml_output.push_str(&format!(
                    "total_formatted = \"{}\"\n",
                    format_hours(month_total)
                ));

                // By person breakdown as nested array
                for (person, mins) in summary.by_person() {
                    toml_output.push_str("\n[[months.by_person]]\n");
                    toml_output.push_str(&format!("person = \"{}\"\n", person));
                    toml_output.push_str(&format!("minutes = {}\n", mins));
                    toml_output.push_str(&format!("formatted = \"{}\"\n", format_hours(mins)));
                }

                // Events (only if not quiet/sum_only)
                if !args.quiet && !args.silent && !args.sum_only {
                    for event in &summary.events {
                        if let Some(mins) = event_duration_minutes(event) {
                            toml_output.push_str("\n[[months.events]]\n");
                            toml_output.push_str(&format!(
                                "summary = \"{}\"\n",
                                toml_escape(&event.summary)
                            ));
                            toml_output.push_str(&format!(
                                "start = \"{}\"\n",
                                event.start.format("%Y-%m-%d %H:%M")
                            ));
                            toml_output.push_str(&format!(
                                "end = \"{}\"\n",
                                event.end.format("%Y-%m-%d %H:%M")
                            ));
                            toml_output.push_str(&format!("duration_minutes = {}\n", mins));
                            toml_output.push_str(&format!(
                                "duration_formatted = \"{}\"\n",
                                format_hours(mins)
                            ));
                        }
                    }
                }
            }
            writeln!(out_writer, "{}", toml_output)?;
        }
        OutputFormat::Pivot => {
            // Pivot table: person vs weekday - hours matrix
            let weekday_names = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"];
            let mut by_person_weekday: BTreeMap<String, [i64; 7]> = BTreeMap::new();
            let mut all_persons: BTreeSet<String> = BTreeSet::new();

            for event in &filtered {
                if let Some(mins) = event_duration_minutes(event) {
                    let person = extract_person(&event.summary)
                        .unwrap_or("(unknown)")
                        .to_string();
                    let wd = event.start.weekday().num_days_from_monday() as usize;
                    all_persons.insert(person.clone());
                    by_person_weekday.entry(person).or_default()[wd] += mins;
                }
            }

            // Header row
            write!(out_writer, "{:<20}", "").ok();
            for day in &weekday_names {
                write!(out_writer, " {:>10}", day).ok();
            }
            writeln!(out_writer, " {:>10}", "Total").ok();

            // Separator
            write!(out_writer, "{:<20}", "---").ok();
            for _ in &weekday_names {
                write!(out_writer, " {:>10}", "---").ok();
            }
            writeln!(out_writer, " {:>10}", "---").ok();

            // Data rows
            let mut grand_totals: [i64; 7] = [0; 7];
            let mut grand_total: i64 = 0;
            for person in &all_persons {
                let hours = by_person_weekday.get(person).copied().unwrap_or([0; 7]);
                let row_total: i64 = hours.iter().sum();
                write!(out_writer, "{:<20}", person).ok();
                for (i, day_hours) in hours.iter().enumerate() {
                    grand_totals[i] += day_hours;
                    if *day_hours > 0 {
                        write!(out_writer, " {:>10}", format_hours(*day_hours)).ok();
                    } else {
                        write!(out_writer, " {:>10}", "-").ok();
                    }
                }
                grand_total += row_total;
                writeln!(out_writer, " {:>10}", format_hours(row_total)).ok();
            }

            // Grand total row
            write!(out_writer, "{:<20}", colored(color::BOLD, "TOTAL")).ok();
            for day_total in &grand_totals {
                if *day_total > 0 {
                    write!(
                        out_writer,
                        " {:>10}",
                        colored(color::YELLOW, format_hours(*day_total))
                    )
                    .ok();
                } else {
                    write!(out_writer, " {:>10}", "-").ok();
                }
            }
            writeln!(
                out_writer,
                " {:>10}",
                colored(color::GREEN, format_hours(grand_total))
            )
            .ok();
        }
        OutputFormat::Html => {
            // Build per-person summary
            let all_by_person: BTreeMap<&str, i64> = filtered
                .iter()
                .filter_map(|e| {
                    let mins = event_duration_minutes(e)?;
                    Some((extract_person(&e.summary).unwrap_or("(unknown)"), mins))
                })
                .fold(
                    BTreeMap::new(),
                    |mut acc: BTreeMap<&str, i64>, (person, mins)| {
                        *acc.entry(person).or_default() += mins;
                        acc
                    },
                );

            writeln!(out_writer, "<!DOCTYPE html>")?;
            writeln!(out_writer, "<html lang=\"en\">")?;
            writeln!(out_writer, "<head>")?;
            writeln!(out_writer, "  <meta charset=\"UTF-8\">")?;
            writeln!(
                out_writer,
                "  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\">"
            )?;
            writeln!(
                out_writer,
                "  <title>Time Report - proton-extractor</title>"
            )?;
            writeln!(out_writer, "  <style>")?;
            writeln!(out_writer, "    body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; max-width: 800px; margin: 0 auto; padding: 20px; background: #f5f5f5; }}")?;
            writeln!(
                out_writer,
                "    h1 {{ color: #333; border-bottom: 2px solid #4CAF50; padding-bottom: 10px; }}"
            )?;
            writeln!(out_writer, "    h2 {{ color: #555; margin-top: 30px; }}")?;
            writeln!(out_writer, "    .month-section {{ background: white; border-radius: 8px; padding: 20px; margin: 20px 0; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }}")?;
            writeln!(
                out_writer,
                "    .event-list {{ list-style: none; padding: 0; }}"
            )?;
            writeln!(out_writer, "    .event-item {{ padding: 8px 0; border-bottom: 1px solid #eee; display: flex; justify-content: space-between; }}")?;
            writeln!(
                out_writer,
                "    .event-item:last-child {{ border-bottom: none; }}"
            )?;
            writeln!(
                out_writer,
                "    .duration {{ font-weight: bold; color: #4CAF50; }}"
            )?;
            writeln!(out_writer, "    .summary {{ color: #333; }}")?;
            writeln!(out_writer, "    .person-summary {{ display: flex; justify-content: space-between; padding: 8px 0; border-bottom: 1px solid #eee; }}")?;
            writeln!(out_writer, "    .total {{ font-weight: bold; font-size: 1.2em; color: #333; padding: 15px 0; border-top: 2px solid #4CAF50; margin-top: 10px; }}")?;
            writeln!(out_writer, "    .grand-total {{ background: #4CAF50; color: white; padding: 20px; border-radius: 8px; margin: 20px 0; text-align: center; font-size: 1.5em; }}")?;
            writeln!(out_writer, "    .person-breakdown {{ background: #f9f9f9; padding: 15px; border-radius: 4px; margin-top: 15px; }}")?;
            writeln!(
                out_writer,
                "    .percentage {{ color: #888; font-size: 0.9em; }}"
            )?;
            writeln!(out_writer, "  </style>")?;
            writeln!(out_writer, "</head>")?;
            writeln!(out_writer, "<body>")?;
            writeln!(out_writer, "  <h1>⏱️ Time Report</h1>")?;
            writeln!(out_writer, "  <p>Generated by <a href=\"https://github.com/JeremySomsouk/proton-extractor\">proton-extractor</a></p>")?;

            for ((_year, _month), summary) in &grouped {
                summary.write_html(
                    &mut out_writer,
                    args.quiet,
                    args.sum_only,
                    grand_total_minutes,
                )?;
            }

            if grand_total_minutes > 0 && !all_by_person.is_empty() && !args.sum_only {
                writeln!(out_writer, "  <div class=\"grand-total\">")?;
                writeln!(
                    out_writer,
                    "    🎯 Grand Total: {}",
                    format_hours(grand_total_minutes)
                )?;
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
                .fold(
                    BTreeMap::new(),
                    |mut acc: BTreeMap<&str, i64>, (person, mins)| {
                        *acc.entry(person).or_default() += mins;
                        acc
                    },
                );

            // Group by person instead of month if --group-by-person is set
            if args.group_by_person {
                let by_person = group_by_person(&filtered);
                for (person, events) in &by_person {
                    let person_total: i64 = events
                        .iter()
                        .filter_map(|e| event_duration_minutes(e))
                        .sum();
                    writeln!(out_writer)?;
                    writeln!(
                        out_writer,
                        "{}",
                        colored(color::CYAN, format!("--- {} ---", person))
                    )?;

                    if !args.quiet && !args.silent && !args.sum_only {
                        for event in events {
                            if let Some(mins) = event_duration_minutes(event) {
                                writeln!(
                                    out_writer,
                                    "  {}  {}",
                                    colored(color::YELLOW, format_hours(mins)),
                                    event.summary
                                )?;
                            }
                        }
                    }
                    writeln!(
                        out_writer,
                        "  {}  {}",
                        colored(color::GREEN, format_hours(person_total)),
                        colored(color::BOLD, "TOTAL")
                    )?;
                }

                if grand_total_minutes > 0 {
                    writeln!(out_writer)?;
                    writeln!(
                        out_writer,
                        "{}",
                        colored(
                            color::GREEN,
                            format!("=== Grand Total: {} ===", format_hours(grand_total_minutes))
                        )
                    )?;
                }
            } else if args.group_by_project {
                // Group by project instead of month if --group-by-project is set
                let by_project = group_by_project(&filtered);
                for (project, events) in &by_project {
                    let project_total: i64 = events
                        .iter()
                        .filter_map(|e| event_duration_minutes(e))
                        .sum();
                    writeln!(out_writer)?;
                    writeln!(
                        out_writer,
                        "{}",
                        colored(color::CYAN, format!("--- {{{}}} ---", project))
                    )?;

                    if !args.quiet && !args.silent && !args.sum_only {
                        for event in events {
                            if let Some(mins) = event_duration_minutes(event) {
                                writeln!(
                                    out_writer,
                                    "  {}  {}",
                                    colored(color::YELLOW, format_hours(mins)),
                                    event.summary
                                )?;
                            }
                        }
                    }
                    writeln!(
                        out_writer,
                        "  {}  {}",
                        colored(color::GREEN, format_hours(project_total)),
                        colored(color::BOLD, "TOTAL")
                    )?;
                }

                if grand_total_minutes > 0 {
                    writeln!(out_writer)?;
                    writeln!(
                        out_writer,
                        "{}",
                        colored(
                            color::GREEN,
                            format!("=== Grand Total: {} ===", format_hours(grand_total_minutes))
                        )
                    )?;
                }
            } else if args.group_by_weekday {
                // Group by weekday instead of month if --group-by-weekday is set
                let by_weekday = group_by_weekday(&filtered);
                for (day_name, events) in &by_weekday {
                    let day_total: i64 = events
                        .iter()
                        .filter_map(|e| event_duration_minutes(e))
                        .sum();
                    writeln!(out_writer)?;
                    writeln!(
                        out_writer,
                        "{}",
                        colored(color::CYAN, format!("--- {} ---", day_name))
                    )?;

                    if !args.quiet && !args.silent && !args.sum_only {
                        for event in events {
                            if let Some(mins) = event_duration_minutes(event) {
                                writeln!(
                                    out_writer,
                                    "  {}  {}",
                                    colored(color::YELLOW, format_hours(mins)),
                                    event.summary
                                )?;
                            }
                        }
                    }
                    writeln!(
                        out_writer,
                        "  {}  {}",
                        colored(color::GREEN, format_hours(day_total)),
                        colored(color::BOLD, "TOTAL")
                    )?;
                }

                if grand_total_minutes > 0 {
                    writeln!(out_writer)?;
                    writeln!(
                        out_writer,
                        "{}",
                        colored(
                            color::GREEN,
                            format!("=== Grand Total: {} ===", format_hours(grand_total_minutes))
                        )
                    )?;
                }
            } else if args.group_by_location {
                // Group by location instead of month if --group-by-location is set
                let by_location = group_by_location(&filtered);
                for (location, events) in &by_location {
                    let location_total: i64 = events
                        .iter()
                        .filter_map(|e| event_duration_minutes(e))
                        .sum();
                    writeln!(out_writer)?;
                    writeln!(
                        out_writer,
                        "{}",
                        colored(color::CYAN, format!("--- {} ---", location))
                    )?;

                    if !args.quiet && !args.silent && !args.sum_only {
                        for event in events {
                            if let Some(mins) = event_duration_minutes(event) {
                                writeln!(
                                    out_writer,
                                    "  {}  {}",
                                    colored(color::YELLOW, format_hours(mins)),
                                    event.summary
                                )?;
                            }
                        }
                    }
                    writeln!(
                        out_writer,
                        "  {}  {}",
                        colored(color::GREEN, format_hours(location_total)),
                        colored(color::BOLD, "TOTAL")
                    )?;
                }

                if grand_total_minutes > 0 {
                    writeln!(out_writer)?;
                    writeln!(
                        out_writer,
                        "{}",
                        colored(
                            color::GREEN,
                            format!("=== Grand Total: {} ===", format_hours(grand_total_minutes))
                        )
                    )?;
                }
            } else if args.group_by_category {
                // Group by category instead of month if --group-by-category is set
                let by_category = group_by_category(&filtered);
                for (category, events) in &by_category {
                    let category_total: i64 = events
                        .iter()
                        .filter_map(|e| event_duration_minutes(e))
                        .sum();
                    writeln!(out_writer)?;
                    writeln!(
                        out_writer,
                        "{}",
                        colored(color::CYAN, format!("--- {} ---", category))
                    )?;

                    if !args.quiet && !args.silent && !args.sum_only {
                        for event in events {
                            if let Some(mins) = event_duration_minutes(event) {
                                writeln!(
                                    out_writer,
                                    "  {}  {}",
                                    colored(color::YELLOW, format_hours(mins)),
                                    event.summary
                                )?;
                            }
                        }
                    }
                    writeln!(
                        out_writer,
                        "  {}  {}",
                        colored(color::GREEN, format_hours(category_total)),
                        colored(color::BOLD, "TOTAL")
                    )?;
                }

                if grand_total_minutes > 0 {
                    writeln!(out_writer)?;
                    writeln!(
                        out_writer,
                        "{}",
                        colored(
                            color::GREEN,
                            format!("=== Grand Total: {} ===", format_hours(grand_total_minutes))
                        )
                    )?;
                }
            } else if args.group_by_year {
                // Group by year instead of month if --group-by-year is set
                let by_year = group_by_year(&filtered);
                for (year, events) in &by_year {
                    let year_total: i64 = events
                        .iter()
                        .filter_map(|e| event_duration_minutes(e))
                        .sum();
                    writeln!(out_writer)?;
                    writeln!(
                        out_writer,
                        "{}",
                        colored(color::CYAN, format!("--- {} ---", year))
                    )?;

                    if !args.quiet && !args.silent && !args.sum_only {
                        for event in events {
                            if let Some(mins) = event_duration_minutes(event) {
                                writeln!(
                                    out_writer,
                                    "  {}  {}",
                                    colored(color::YELLOW, format_hours(mins)),
                                    event.summary
                                )?;
                            }
                        }
                    }
                    writeln!(
                        out_writer,
                        "  {}  {}",
                        colored(color::GREEN, format_hours(year_total)),
                        colored(color::BOLD, "TOTAL")
                    )?;
                }

                if grand_total_minutes > 0 {
                    writeln!(out_writer)?;
                    writeln!(
                        out_writer,
                        "{}",
                        colored(
                            color::GREEN,
                            format!("=== Grand Total: {} ===", format_hours(grand_total_minutes))
                        )
                    )?;
                }
            } else {
                for ((year, _month), summary) in &grouped {
                    writeln!(out_writer)?;
                    writeln!(
                        out_writer,
                        "{}",
                        colored(
                            color::CYAN,
                            format!("--- {} {} ---", summary.month_name, year)
                        )
                    )?;

                    let month_by_person = summary.by_person();

                    if !args.quiet && !args.silent && !args.sum_only {
                        for event in &summary.events {
                            if let Some(mins) = event_duration_minutes(event) {
                                writeln!(
                                    out_writer,
                                    "  {}  {}",
                                    colored(color::YELLOW, format_hours(mins)),
                                    event.summary
                                )?;
                            }
                        }
                    }

                    writeln!(out_writer, "  {}", colored(color::MAGENTA, "------"))?;
                    for (person, mins) in &month_by_person {
                        writeln!(
                            out_writer,
                            "  {}  {}",
                            colored(color::YELLOW, format_hours(*mins)),
                            person
                        )?;
                    }
                    writeln!(
                        out_writer,
                        "  {}  {}",
                        colored(color::GREEN, format_hours(summary.total_minutes())),
                        colored(color::BOLD, "TOTAL")
                    )?;
                }

                if grand_total_minutes > 0 && grouped.len() > 1 {
                    writeln!(out_writer)?;
                    writeln!(
                        out_writer,
                        "{}",
                        colored(
                            color::GREEN,
                            format!("=== Grand Total: {} ===", format_hours(grand_total_minutes))
                        )
                    )?;
                }

                if !all_by_person.is_empty() && !args.sum_only {
                    writeln!(out_writer)?;
                    writeln!(
                        out_writer,
                        "{}",
                        colored(color::CYAN, "=== Hours per person ===")
                    )?;
                    for (person, mins) in &all_by_person {
                        writeln!(
                            out_writer,
                            "  {}  {:>6}  {}",
                            colored(color::YELLOW, format_hours(*mins)),
                            colored(
                                color::MAGENTA,
                                format_percentage(*mins, grand_total_minutes)
                            ),
                            person
                        )?;
                    }
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
                .fold(
                    BTreeMap::new(),
                    |mut acc: BTreeMap<&str, i64>, (person, mins)| {
                        *acc.entry(person).or_default() += mins;
                        acc
                    },
                );

            for ((year, _month), summary) in &grouped {
                writeln!(out_writer)?;
                writeln!(out_writer, "## {} {}", summary.month_name, year)?;
                writeln!(out_writer)?;

                let month_by_person = summary.by_person();

                if !args.quiet && !args.silent && !args.sum_only {
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
                    writeln!(
                        out_writer,
                        "| {} | {} | {} |",
                        person,
                        format_hours(*mins),
                        format_percentage(*mins, summary.total_minutes())
                    )?;
                }
                writeln!(
                    out_writer,
                    "| **TOTAL** | **{}** | 100% |",
                    format_hours(summary.total_minutes())
                )?;
                writeln!(out_writer)?;
            }

            if grand_total_minutes > 0 && !all_by_person.is_empty() {
                writeln!(
                    out_writer,
                    "## 🎯 Grand Total: {}",
                    format_hours(grand_total_minutes)
                )?;
                writeln!(out_writer)?;
                writeln!(out_writer, "### 👥 Hours per Person")?;
                writeln!(out_writer)?;
                writeln!(out_writer, "| Person | Hours | % |")?;
                writeln!(out_writer, "|--------|-------|---|")?;
                for (person, mins) in &all_by_person {
                    writeln!(
                        out_writer,
                        "| {} | {} | {} |",
                        person,
                        format_hours(*mins),
                        format_percentage(*mins, grand_total_minutes)
                    )?;
                }
            }
        }
    }

    // Success feedback when writing to file
    if write_to_file {
        if let Some(ref path) = output_file_path {
            let event_count = filtered.len();
            print_exported(event_count, path);
            if path.extension().map(|e| e == "html").unwrap_or(false) {
                print_hint(format!("Open in browser: open {}", path.display()));
            } else {
                print_hint(format!("View: cat {}", path.display()));
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
    fn test_matches_recent_filter() {
        let today = NaiveDate::from_ymd_opt(2024, 3, 15).unwrap();

        // Event on today
        let today_event = Event::new(
            "Today meeting [Alice]".to_string(),
            today.and_hms_opt(9, 0, 0).unwrap(),
            today.and_hms_opt(10, 0, 0).unwrap(),
        );

        // Event 2 days ago
        let two_days_ago = today - Duration::days(2);
        let two_days_ago_event = Event::new(
            "Two days ago [Bob]".to_string(),
            two_days_ago.and_hms_opt(9, 0, 0).unwrap(),
            two_days_ago.and_hms_opt(10, 0, 0).unwrap(),
        );

        // Event 5 days ago
        let five_days_ago = today - Duration::days(5);
        let five_days_ago_event = Event::new(
            "Five days ago [Carol]".to_string(),
            five_days_ago.and_hms_opt(9, 0, 0).unwrap(),
            five_days_ago.and_hms_opt(10, 0, 0).unwrap(),
        );

        // Event 10 days ago
        let ten_days_ago = today - Duration::days(10);
        let ten_days_ago_event = Event::new(
            "Ten days ago [Dave]".to_string(),
            ten_days_ago.and_hms_opt(9, 0, 0).unwrap(),
            ten_days_ago.and_hms_opt(10, 0, 0).unwrap(),
        );

        // No filter = all pass
        assert!(matches_recent_filter(&today_event, &None, &today));
        assert!(matches_recent_filter(&two_days_ago_event, &None, &today));
        assert!(matches_recent_filter(&five_days_ago_event, &None, &today));

        // --recent 3: today, yesterday, 2 days ago (3 days inclusive)
        let recent_3 = Some(3);
        assert!(matches_recent_filter(&today_event, &recent_3, &today));
        assert!(matches_recent_filter(
            &two_days_ago_event,
            &recent_3,
            &today
        ));
        assert!(!matches_recent_filter(
            &five_days_ago_event,
            &recent_3,
            &today
        ));
        assert!(!matches_recent_filter(
            &ten_days_ago_event,
            &recent_3,
            &today
        ));

        // --recent 7: last week inclusive
        let recent_7 = Some(7);
        assert!(matches_recent_filter(&today_event, &recent_7, &today));
        assert!(matches_recent_filter(
            &two_days_ago_event,
            &recent_7,
            &today
        ));
        assert!(matches_recent_filter(
            &five_days_ago_event,
            &recent_7,
            &today
        ));
        assert!(!matches_recent_filter(
            &ten_days_ago_event,
            &recent_7,
            &today
        ));

        // --recent 1: today only
        let recent_1 = Some(1);
        assert!(matches_recent_filter(&today_event, &recent_1, &today));
        assert!(!matches_recent_filter(
            &two_days_ago_event,
            &recent_1,
            &today
        ));
    }

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
        let (freq, until, byday, interval, count, bymonthday) = result.unwrap();
        assert_eq!(freq, "WEEKLY");
        assert_eq!(until.format("%Y%m%d").to_string(), "20240315"); // Date only, time is 00:00:00
        assert!(byday.is_none());
        assert_eq!(interval, None);
        assert_eq!(count, None);
        assert!(bymonthday.is_none());

        // BYDAY extraction
        let result = parse_rrule("FREQ=WEEKLY;BYDAY=MO,WE,FR;UNTIL=20240315T090000Z");
        assert!(result.is_some());
        let (freq, _, byday, _, _, _) = result.unwrap();
        assert_eq!(freq, "WEEKLY");
        assert_eq!(
            byday,
            Some(vec!["MO".to_string(), "WE".to_string(), "FR".to_string()])
        );

        // Missing UNTIL gets a default
        let result = parse_rrule("FREQ=DAILY");
        assert!(result.is_some());
        let (_, until, _, _, _, _) = result.unwrap();
        assert_eq!(until.format("%Y").to_string(), "2099"); // Should have default date

        // INTERVAL extraction
        let result = parse_rrule("FREQ=WEEKLY;INTERVAL=2;UNTIL=20240315T090000Z");
        assert!(result.is_some());
        let (freq, _, _, interval, _, _) = result.unwrap();
        assert_eq!(freq, "WEEKLY");
        assert_eq!(interval, Some(2));

        // COUNT extraction
        let result = parse_rrule("FREQ=DAILY;COUNT=10");
        assert!(result.is_some());
        let (_, _, _, _, count, _) = result.unwrap();
        assert_eq!(count, Some(10));

        // BYMONTHDAY extraction
        let result = parse_rrule("FREQ=MONTHLY;BYMONTHDAY=15;UNTIL=20241231T235959");
        assert!(result.is_some());
        let (_, _, _, _, _, bymonthday) = result.unwrap();
        assert_eq!(bymonthday, Some(vec![15]));

        // Multiple BYMONTHDAY
        let result = parse_rrule("FREQ=MONTHLY;BYMONTHDAY=1,-1;UNTIL=20241231T235959");
        assert!(result.is_some());
        let (_, _, _, _, _, bymonthday) = result.unwrap();
        assert_eq!(bymonthday, Some(vec![1, -1]));
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
        assert_eq!(
            parse_duration("PT1H30M"),
            Some(Duration::hours(1) + Duration::minutes(30))
        );

        // Combined
        assert_eq!(
            parse_duration("P1DT1H"),
            Some(Duration::days(1) + Duration::hours(1))
        );
        assert_eq!(
            parse_duration("P1DT1H30M"),
            Some(Duration::days(1) + Duration::hours(1) + Duration::minutes(30))
        );

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
            NaiveDate::from_ymd_opt(2024, 3, 15)
                .unwrap()
                .and_hms_opt(9, 0, 0)
                .unwrap(),
            NaiveDate::from_ymd_opt(2024, 3, 15)
                .unwrap()
                .and_hms_opt(10, 0, 0)
                .unwrap(),
        );

        assert!(matches_person_filter(&event, &None));
        assert!(matches_person_filter(&event, &Some("John".to_string())));
        assert!(matches_person_filter(&event, &Some("john".to_string())));
        assert!(!matches_person_filter(&event, &Some("Jane".to_string())));
    }

    #[test]
    fn test_matches_persons_filter() {
        let alice_event = Event::new(
            "Meeting with [Alice]".to_string(),
            NaiveDate::from_ymd_opt(2024, 3, 15)
                .unwrap()
                .and_hms_opt(9, 0, 0)
                .unwrap(),
            NaiveDate::from_ymd_opt(2024, 3, 15)
                .unwrap()
                .and_hms_opt(10, 0, 0)
                .unwrap(),
        );
        let bob_event = Event::new(
            "Meeting with [Bob]".to_string(),
            NaiveDate::from_ymd_opt(2024, 3, 15)
                .unwrap()
                .and_hms_opt(10, 0, 0)
                .unwrap(),
            NaiveDate::from_ymd_opt(2024, 3, 15)
                .unwrap()
                .and_hms_opt(11, 0, 0)
                .unwrap(),
        );
        let carol_event = Event::new(
            "Meeting with [Carol]".to_string(),
            NaiveDate::from_ymd_opt(2024, 3, 15)
                .unwrap()
                .and_hms_opt(11, 0, 0)
                .unwrap(),
            NaiveDate::from_ymd_opt(2024, 3, 15)
                .unwrap()
                .and_hms_opt(12, 0, 0)
                .unwrap(),
        );

        // Empty list matches all
        assert!(matches_persons_filter(&alice_event, &[]));
        assert!(matches_persons_filter(&bob_event, &[]));

        // Single person (OR with 1 element = same as regular person filter)
        assert!(matches_persons_filter(&alice_event, &["Alice".to_string()]));
        assert!(matches_persons_filter(&alice_event, &["alice".to_string()])); // case insensitive
        assert!(!matches_persons_filter(&alice_event, &["Bob".to_string()]));

        // Multiple persons (OR logic)
        assert!(matches_persons_filter(
            &alice_event,
            &["Alice".to_string(), "Bob".to_string()]
        ));
        assert!(matches_persons_filter(
            &bob_event,
            &["Alice".to_string(), "Bob".to_string()]
        ));
        assert!(!matches_persons_filter(
            &carol_event,
            &["Alice".to_string(), "Bob".to_string()]
        ));

        // Case insensitive
        assert!(matches_persons_filter(
            &alice_event,
            &["ALICE".to_string(), "bob".to_string()]
        ));

        // Partial match
        assert!(matches_persons_filter(
            &alice_event,
            &["Ali".to_string(), "Carol".to_string()]
        ));

        // Event without person doesn't match
        let no_person = Event::new(
            "Regular meeting".to_string(),
            NaiveDate::from_ymd_opt(2024, 3, 15)
                .unwrap()
                .and_hms_opt(9, 0, 0)
                .unwrap(),
            NaiveDate::from_ymd_opt(2024, 3, 15)
                .unwrap()
                .and_hms_opt(10, 0, 0)
                .unwrap(),
        );
        assert!(!matches_persons_filter(&no_person, &["Alice".to_string()]));
    }

    #[test]
    fn test_matches_project_filter() {
        let event = Event::new(
            "Meeting {Project Alpha}".to_string(),
            NaiveDate::from_ymd_opt(2024, 3, 15)
                .unwrap()
                .and_hms_opt(9, 0, 0)
                .unwrap(),
            NaiveDate::from_ymd_opt(2024, 3, 15)
                .unwrap()
                .and_hms_opt(10, 0, 0)
                .unwrap(),
        );

        assert!(matches_project_filter(&event, &None));
        assert!(matches_project_filter(&event, &Some("Alpha".to_string())));
        assert!(matches_project_filter(&event, &Some("alpha".to_string())));
        assert!(matches_project_filter(&event, &Some("Project".to_string())));
        assert!(!matches_project_filter(&event, &Some("Beta".to_string())));

        // Event without project
        let no_project = Event::new(
            "Regular meeting".to_string(),
            NaiveDate::from_ymd_opt(2024, 3, 15)
                .unwrap()
                .and_hms_opt(9, 0, 0)
                .unwrap(),
            NaiveDate::from_ymd_opt(2024, 3, 15)
                .unwrap()
                .and_hms_opt(10, 0, 0)
                .unwrap(),
        );
        assert!(!matches_project_filter(
            &no_project,
            &Some("anything".to_string())
        ));
    }

    #[test]
    fn test_matches_tag_filter() {
        // Event with person only
        let person_event = Event::new(
            "Meeting with [John Doe]".to_string(),
            NaiveDate::from_ymd_opt(2024, 3, 15)
                .unwrap()
                .and_hms_opt(9, 0, 0)
                .unwrap(),
            NaiveDate::from_ymd_opt(2024, 3, 15)
                .unwrap()
                .and_hms_opt(10, 0, 0)
                .unwrap(),
        );

        // Event with project only
        let project_event = Event::new(
            "Meeting {Alpha}".to_string(),
            NaiveDate::from_ymd_opt(2024, 3, 15)
                .unwrap()
                .and_hms_opt(9, 0, 0)
                .unwrap(),
            NaiveDate::from_ymd_opt(2024, 3, 15)
                .unwrap()
                .and_hms_opt(10, 0, 0)
                .unwrap(),
        );

        // Event with both
        let both_event = Event::new(
            "Meeting [Alice] {Beta}".to_string(),
            NaiveDate::from_ymd_opt(2024, 3, 15)
                .unwrap()
                .and_hms_opt(9, 0, 0)
                .unwrap(),
            NaiveDate::from_ymd_opt(2024, 3, 15)
                .unwrap()
                .and_hms_opt(10, 0, 0)
                .unwrap(),
        );

        // Event with neither
        let neither_event = Event::new(
            "Regular meeting".to_string(),
            NaiveDate::from_ymd_opt(2024, 3, 15)
                .unwrap()
                .and_hms_opt(9, 0, 0)
                .unwrap(),
            NaiveDate::from_ymd_opt(2024, 3, 15)
                .unwrap()
                .and_hms_opt(10, 0, 0)
                .unwrap(),
        );

        // No filter = all match
        assert!(matches_tag_filter(&person_event, &None));
        assert!(matches_tag_filter(&project_event, &None));
        assert!(matches_tag_filter(&both_event, &None));
        assert!(matches_tag_filter(&neither_event, &None));

        // Tag matches person
        assert!(matches_tag_filter(&person_event, &Some("John".to_string())));
        assert!(matches_tag_filter(&person_event, &Some("john".to_string()))); // case insensitive
        assert!(!matches_tag_filter(
            &person_event,
            &Some("Alice".to_string())
        ));

        // Tag matches project
        assert!(matches_tag_filter(
            &project_event,
            &Some("Alpha".to_string())
        ));
        assert!(matches_tag_filter(
            &project_event,
            &Some("alpha".to_string())
        )); // case insensitive
        assert!(!matches_tag_filter(
            &project_event,
            &Some("Beta".to_string())
        ));

        // Tag matches either person or project
        assert!(matches_tag_filter(&both_event, &Some("Alice".to_string())));
        assert!(matches_tag_filter(&both_event, &Some("Beta".to_string())));
        assert!(!matches_tag_filter(
            &both_event,
            &Some("Charlie".to_string())
        )); // not in either

        // No person or project = no match
        assert!(!matches_tag_filter(
            &neither_event,
            &Some("anything".to_string())
        ));
    }

    #[test]
    fn test_matches_date_range() {
        let event = Event::new(
            "Test [Event]".to_string(),
            NaiveDate::from_ymd_opt(2024, 3, 15)
                .unwrap()
                .and_hms_opt(9, 0, 0)
                .unwrap(),
            NaiveDate::from_ymd_opt(2024, 3, 15)
                .unwrap()
                .and_hms_opt(10, 0, 0)
                .unwrap(),
        );

        assert!(matches_date_range(&event, &None, &None));
        assert!(matches_date_range(
            &event,
            &Some(NaiveDate::from_ymd_opt(2024, 3, 1).unwrap()),
            &None
        ));
        assert!(matches_date_range(
            &event,
            &None,
            &Some(NaiveDate::from_ymd_opt(2024, 3, 31).unwrap())
        ));
        assert!(!matches_date_range(
            &event,
            &Some(NaiveDate::from_ymd_opt(2024, 4, 1).unwrap()),
            &None
        ));
        assert!(!matches_date_range(
            &event,
            &None,
            &Some(NaiveDate::from_ymd_opt(2024, 3, 1).unwrap())
        ));
    }

    #[test]
    fn test_matches_filter_today() {
        let today = Local::now().naive_local().date();
        let now = today.and_hms_opt(12, 0, 0).unwrap();
        let yesterday_dt = (today - chrono::Duration::days(1))
            .and_hms_opt(12, 0, 0)
            .unwrap();
        let tomorrow_dt = (today + chrono::Duration::days(1))
            .and_hms_opt(12, 0, 0)
            .unwrap();
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
        let tomorrow = today + chrono::Duration::days(1);
        let tomorrow_event = Event::new(
            "Tomorrow meeting [Carol]".to_string(),
            tomorrow.and_hms_opt(9, 0, 0).unwrap(),
            tomorrow.and_hms_opt(10, 0, 0).unwrap(),
        );

        assert!(matches_filter(
            &today_event,
            &DateFilter::Today,
            &now,
            &yesterday_dt,
            &tomorrow_dt
        ));
        assert!(!matches_filter(
            &yesterday_event,
            &DateFilter::Today,
            &now,
            &yesterday_dt,
            &tomorrow_dt
        ));
        assert!(!matches_filter(
            &tomorrow_event,
            &DateFilter::Today,
            &now,
            &yesterday_dt,
            &tomorrow_dt
        ));
        assert!(matches_filter(
            &today_event,
            &DateFilter::All,
            &now,
            &yesterday_dt,
            &tomorrow_dt
        ));
        assert!(matches_filter(
            &yesterday_event,
            &DateFilter::All,
            &now,
            &yesterday_dt,
            &tomorrow_dt
        ));
        assert!(matches_filter(
            &tomorrow_event,
            &DateFilter::All,
            &now,
            &yesterday_dt,
            &tomorrow_dt
        ));
    }

    #[test]
    fn test_matches_filter_yesterday() {
        let today = Local::now().naive_local().date();
        let now = today.and_hms_opt(12, 0, 0).unwrap();
        let yesterday_dt = (today - chrono::Duration::days(1))
            .and_hms_opt(12, 0, 0)
            .unwrap();
        let tomorrow_dt = (today + chrono::Duration::days(1))
            .and_hms_opt(12, 0, 0)
            .unwrap();
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
        let two_days_ago = today - chrono::Duration::days(2);
        let two_days_ago_event = Event::new(
            "Two days ago [Carol]".to_string(),
            two_days_ago.and_hms_opt(9, 0, 0).unwrap(),
            two_days_ago.and_hms_opt(10, 0, 0).unwrap(),
        );

        assert!(matches_filter(
            &yesterday_event,
            &DateFilter::Yesterday,
            &now,
            &yesterday_dt,
            &tomorrow_dt
        ));
        assert!(!matches_filter(
            &today_event,
            &DateFilter::Yesterday,
            &now,
            &yesterday_dt,
            &tomorrow_dt
        ));
        assert!(!matches_filter(
            &two_days_ago_event,
            &DateFilter::Yesterday,
            &now,
            &yesterday_dt,
            &tomorrow_dt
        ));
    }

    #[test]
    fn test_matches_filter_tomorrow() {
        let today = Local::now().naive_local().date();
        let now = today.and_hms_opt(12, 0, 0).unwrap();
        let yesterday_dt = (today - chrono::Duration::days(1))
            .and_hms_opt(12, 0, 0)
            .unwrap();
        let tomorrow_dt = (today + chrono::Duration::days(1))
            .and_hms_opt(12, 0, 0)
            .unwrap();
        let today_event = Event::new(
            "Today meeting [Alice]".to_string(),
            today.and_hms_opt(9, 0, 0).unwrap(),
            today.and_hms_opt(10, 0, 0).unwrap(),
        );
        let tomorrow = today + chrono::Duration::days(1);
        let tomorrow_event = Event::new(
            "Tomorrow meeting [Bob]".to_string(),
            tomorrow.and_hms_opt(9, 0, 0).unwrap(),
            tomorrow.and_hms_opt(10, 0, 0).unwrap(),
        );
        let two_days_future = today + chrono::Duration::days(2);
        let two_days_future_event = Event::new(
            "Two days from now [Carol]".to_string(),
            two_days_future.and_hms_opt(9, 0, 0).unwrap(),
            two_days_future.and_hms_opt(10, 0, 0).unwrap(),
        );

        assert!(matches_filter(
            &tomorrow_event,
            &DateFilter::Tomorrow,
            &now,
            &yesterday_dt,
            &tomorrow_dt
        ));
        assert!(!matches_filter(
            &today_event,
            &DateFilter::Tomorrow,
            &now,
            &yesterday_dt,
            &tomorrow_dt
        ));
        assert!(!matches_filter(
            &two_days_future_event,
            &DateFilter::Tomorrow,
            &now,
            &yesterday_dt,
            &tomorrow_dt
        ));
    }

    #[test]
    fn test_matches_filter_week() {
        let today = Local::now().naive_local().date();
        let now = today.and_hms_opt(12, 0, 0).unwrap();
        let yesterday_dt = (today - chrono::Duration::days(1))
            .and_hms_opt(12, 0, 0)
            .unwrap();
        let tomorrow_dt = (today + chrono::Duration::days(1))
            .and_hms_opt(12, 0, 0)
            .unwrap();
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

        assert!(matches_filter(
            &today_event,
            &DateFilter::Week,
            &now,
            &yesterday_dt,
            &tomorrow_dt
        ));
        assert!(!matches_filter(
            &last_week_event,
            &DateFilter::Week,
            &now,
            &yesterday_dt,
            &tomorrow_dt
        ));
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
        assert_eq!(
            csv_escape("has,comma\"and\nnewline"),
            "\"has,comma\"\"and\nnewline\""
        );
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
            NaiveDate::from_ymd_opt(2024, 3, 15)
                .unwrap()
                .and_hms_opt(9, 0, 0)
                .unwrap(),
            NaiveDate::from_ymd_opt(2024, 3, 15)
                .unwrap()
                .and_hms_opt(10, 0, 0)
                .unwrap(),
        );
        assert_eq!(event_duration_minutes(&event), Some(60));

        // Zero duration
        let zero = Event::new(
            "Zero".to_string(),
            NaiveDate::from_ymd_opt(2024, 3, 15)
                .unwrap()
                .and_hms_opt(9, 0, 0)
                .unwrap(),
            NaiveDate::from_ymd_opt(2024, 3, 15)
                .unwrap()
                .and_hms_opt(9, 0, 0)
                .unwrap(),
        );
        assert_eq!(event_duration_minutes(&zero), None);

        // Negative duration
        let neg = Event::new(
            "Neg".to_string(),
            NaiveDate::from_ymd_opt(2024, 3, 15)
                .unwrap()
                .and_hms_opt(10, 0, 0)
                .unwrap(),
            NaiveDate::from_ymd_opt(2024, 3, 15)
                .unwrap()
                .and_hms_opt(9, 0, 0)
                .unwrap(),
        );
        assert_eq!(event_duration_minutes(&neg), None);
    }

    #[test]
    fn test_expand_events_simple() {
        let raw = RawEvent {
            summary: "Meeting [Alice]".to_string(),
            start: NaiveDate::from_ymd_opt(2024, 3, 1)
                .unwrap()
                .and_hms_opt(9, 0, 0)
                .unwrap(),
            end: NaiveDate::from_ymd_opt(2024, 3, 1)
                .unwrap()
                .and_hms_opt(10, 0, 0)
                .unwrap(),
            uid: "uid1".to_string(),
            rrule: None,
            exdates: vec![],
            recurrence_id: None,
            location: None,
            categories: vec![],
            status: None,
            source_file: None,
        };
        let expanded = expand_events(vec![raw]);
        assert_eq!(expanded.len(), 1);
        assert_eq!(expanded[0].summary, "Meeting [Alice]");
    }

    #[test]
    fn test_expand_events_filters_zero_duration() {
        let zero_duration = RawEvent {
            summary: "Zero [Bob]".to_string(),
            start: NaiveDate::from_ymd_opt(2024, 3, 1)
                .unwrap()
                .and_hms_opt(9, 0, 0)
                .unwrap(),
            end: NaiveDate::from_ymd_opt(2024, 3, 1)
                .unwrap()
                .and_hms_opt(9, 0, 0)
                .unwrap(),
            uid: "uid1".to_string(),
            rrule: None,
            exdates: vec![],
            recurrence_id: None,
            location: None,
            categories: vec![],
            status: None,
            source_file: None,
        };
        let expanded = expand_events(vec![zero_duration]);
        assert!(expanded.is_empty());
    }

    #[test]
    fn test_expand_events_daily_recurrence() {
        let daily = RawEvent {
            summary: "Daily [Carol]".to_string(),
            start: NaiveDate::from_ymd_opt(2024, 3, 1)
                .unwrap()
                .and_hms_opt(9, 0, 0)
                .unwrap(),
            end: NaiveDate::from_ymd_opt(2024, 3, 1)
                .unwrap()
                .and_hms_opt(10, 0, 0)
                .unwrap(),
            uid: "uid1".to_string(),
            rrule: Some("FREQ=DAILY;UNTIL=20240305T235959".to_string()),
            exdates: vec![],
            recurrence_id: None,
            location: None,
            categories: vec![],
            status: None,
            source_file: None,
        };
        let expanded = expand_events(vec![daily]);
        // 5 days: March 1, 2, 3, 4, 5
        assert_eq!(expanded.len(), 5);
        assert_eq!(
            expanded[0].start.date(),
            NaiveDate::from_ymd_opt(2024, 3, 1).unwrap()
        );
        assert_eq!(
            expanded[4].start.date(),
            NaiveDate::from_ymd_opt(2024, 3, 5).unwrap()
        );
    }

    #[test]
    fn test_expand_events_with_exdates() {
        let with_exdate = RawEvent {
            summary: "Weekly [Dave]".to_string(),
            start: NaiveDate::from_ymd_opt(2024, 3, 1)
                .unwrap()
                .and_hms_opt(9, 0, 0)
                .unwrap(),
            end: NaiveDate::from_ymd_opt(2024, 3, 1)
                .unwrap()
                .and_hms_opt(10, 0, 0)
                .unwrap(),
            uid: "uid1".to_string(),
            rrule: Some("FREQ=WEEKLY;UNTIL=20240315T235959".to_string()),
            exdates: vec![NaiveDate::from_ymd_opt(2024, 3, 8).unwrap()],
            recurrence_id: None,
            location: None,
            categories: vec![],
            status: None,
            source_file: None,
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
            start: NaiveDate::from_ymd_opt(2024, 3, 1)
                .unwrap()
                .and_hms_opt(9, 0, 0)
                .unwrap(),
            end: NaiveDate::from_ymd_opt(2024, 3, 1)
                .unwrap()
                .and_hms_opt(10, 0, 0)
                .unwrap(),
            uid: "uid1".to_string(),
            rrule: Some("FREQ=WEEKLY;INTERVAL=2;UNTIL=20240331T235959".to_string()),
            exdates: vec![],
            recurrence_id: None,
            location: None,
            categories: vec![],
            status: None,
            source_file: None,
        };
        let expanded = expand_events(vec![biweekly]);
        assert_eq!(expanded.len(), 3);
        assert_eq!(
            expanded[0].start.date(),
            NaiveDate::from_ymd_opt(2024, 3, 1).unwrap()
        );
        assert_eq!(
            expanded[1].start.date(),
            NaiveDate::from_ymd_opt(2024, 3, 15).unwrap()
        );
        assert_eq!(
            expanded[2].start.date(),
            NaiveDate::from_ymd_opt(2024, 3, 29).unwrap()
        );
    }

    #[test]
    fn test_expand_events_with_count() {
        // Daily for 5 occurrences
        let daily_count = RawEvent {
            summary: "Daily 5 times [Eve]".to_string(),
            start: NaiveDate::from_ymd_opt(2024, 3, 1)
                .unwrap()
                .and_hms_opt(9, 0, 0)
                .unwrap(),
            end: NaiveDate::from_ymd_opt(2024, 3, 1)
                .unwrap()
                .and_hms_opt(10, 0, 0)
                .unwrap(),
            uid: "uid1".to_string(),
            rrule: Some("FREQ=DAILY;COUNT=5".to_string()),
            exdates: vec![],
            recurrence_id: None,
            location: None,
            categories: vec![],
            status: None,
            source_file: None,
        };
        let expanded = expand_events(vec![daily_count]);
        // Should only produce 5 events despite no UNTIL
        assert_eq!(expanded.len(), 5);
        assert_eq!(
            expanded[0].start.date(),
            NaiveDate::from_ymd_opt(2024, 3, 1).unwrap()
        );
        assert_eq!(
            expanded[4].start.date(),
            NaiveDate::from_ymd_opt(2024, 3, 5).unwrap()
        );
    }

    #[test]
    fn test_expand_events_interval_and_count_combined() {
        // Every 3 days, 4 occurrences max
        let combined = RawEvent {
            summary: "Every 3 days [Frank]".to_string(),
            start: NaiveDate::from_ymd_opt(2024, 3, 1)
                .unwrap()
                .and_hms_opt(9, 0, 0)
                .unwrap(),
            end: NaiveDate::from_ymd_opt(2024, 3, 1)
                .unwrap()
                .and_hms_opt(10, 0, 0)
                .unwrap(),
            uid: "uid1".to_string(),
            rrule: Some("FREQ=DAILY;INTERVAL=3;COUNT=4".to_string()),
            exdates: vec![],
            recurrence_id: None,
            location: None,
            categories: vec![],
            status: None,
            source_file: None,
        };
        let expanded = expand_events(vec![combined]);
        assert_eq!(expanded.len(), 4);
        assert_eq!(
            expanded[0].start.date(),
            NaiveDate::from_ymd_opt(2024, 3, 1).unwrap()
        );
        assert_eq!(
            expanded[1].start.date(),
            NaiveDate::from_ymd_opt(2024, 3, 4).unwrap()
        );
        assert_eq!(
            expanded[2].start.date(),
            NaiveDate::from_ymd_opt(2024, 3, 7).unwrap()
        );
        assert_eq!(
            expanded[3].start.date(),
            NaiveDate::from_ymd_opt(2024, 3, 10).unwrap()
        );
    }

    #[test]
    fn test_matches_exclude_filter() {
        let event = Event::new(
            "Meeting with [John Doe]".to_string(),
            NaiveDate::from_ymd_opt(2024, 3, 15)
                .unwrap()
                .and_hms_opt(9, 0, 0)
                .unwrap(),
            NaiveDate::from_ymd_opt(2024, 3, 15)
                .unwrap()
                .and_hms_opt(10, 0, 0)
                .unwrap(),
        );

        // Empty exclude list should include
        assert!(matches_exclude_filter(&event, &[]));

        // Excluding different person should include
        assert!(matches_exclude_filter(&event, &["Jane".to_string()]));

        // Excluding matching person should exclude
        assert!(!matches_exclude_filter(&event, &["John".to_string()]));
        assert!(!matches_exclude_filter(&event, &["john".to_string()])); // case insensitive

        // Multiple exclude filters
        assert!(!matches_exclude_filter(
            &event,
            &["Jane".to_string(), "John".to_string()]
        ));

        // No person in event should be included
        let event_no_person = Event::new(
            "Regular meeting".to_string(),
            NaiveDate::from_ymd_opt(2024, 3, 15)
                .unwrap()
                .and_hms_opt(9, 0, 0)
                .unwrap(),
            NaiveDate::from_ymd_opt(2024, 3, 15)
                .unwrap()
                .and_hms_opt(10, 0, 0)
                .unwrap(),
        );
        assert!(matches_exclude_filter(
            &event_no_person,
            &["anything".to_string()]
        ));
    }

    #[test]
    fn test_matches_exclude_project_filter() {
        let event = Event::new(
            "Meeting {Project Alpha}".to_string(),
            NaiveDate::from_ymd_opt(2024, 3, 15)
                .unwrap()
                .and_hms_opt(9, 0, 0)
                .unwrap(),
            NaiveDate::from_ymd_opt(2024, 3, 15)
                .unwrap()
                .and_hms_opt(10, 0, 0)
                .unwrap(),
        );

        // Empty exclude list should include
        assert!(matches_exclude_project_filter(&event, &[]));

        // Excluding different project should include
        assert!(matches_exclude_project_filter(
            &event,
            &["Beta".to_string()]
        ));

        // Excluding matching project should exclude
        assert!(!matches_exclude_project_filter(
            &event,
            &["Alpha".to_string()]
        ));
        assert!(!matches_exclude_project_filter(
            &event,
            &["alpha".to_string()]
        )); // case insensitive
        assert!(!matches_exclude_project_filter(
            &event,
            &["Project".to_string()]
        )); // partial match

        // No project in event should be included
        let event_no_project = Event::new(
            "Regular meeting".to_string(),
            NaiveDate::from_ymd_opt(2024, 3, 15)
                .unwrap()
                .and_hms_opt(9, 0, 0)
                .unwrap(),
            NaiveDate::from_ymd_opt(2024, 3, 15)
                .unwrap()
                .and_hms_opt(10, 0, 0)
                .unwrap(),
        );
        assert!(matches_exclude_project_filter(
            &event_no_project,
            &["anything".to_string()]
        ));
    }

    #[test]
    fn test_expand_events_monthly_recurrence() {
        let monthly = RawEvent {
            summary: "Monthly [Eve]".to_string(),
            start: NaiveDate::from_ymd_opt(2024, 1, 15)
                .unwrap()
                .and_hms_opt(9, 0, 0)
                .unwrap(),
            end: NaiveDate::from_ymd_opt(2024, 1, 15)
                .unwrap()
                .and_hms_opt(10, 0, 0)
                .unwrap(),
            uid: "uid1".to_string(),
            rrule: Some("FREQ=MONTHLY;UNTIL=20240615T235959".to_string()),
            exdates: vec![],
            recurrence_id: None,
            location: None,
            categories: vec![],
            status: None,
            source_file: None,
        };
        let expanded = expand_events(vec![monthly]);
        // 6 months: Jan, Feb, Mar, Apr, May, Jun
        assert_eq!(expanded.len(), 6);
        assert_eq!(
            expanded[0].start.date(),
            NaiveDate::from_ymd_opt(2024, 1, 15).unwrap()
        );
        assert_eq!(
            expanded[1].start.date(),
            NaiveDate::from_ymd_opt(2024, 2, 15).unwrap()
        );
        assert_eq!(
            expanded[5].start.date(),
            NaiveDate::from_ymd_opt(2024, 6, 15).unwrap()
        );
    }

    #[test]
    fn test_expand_events_monthly_day_overflow() {
        // Test day overflow handling: Jan 31 -> Feb 28 (non-leap year 2023)
        let monthly_31st = RawEvent {
            summary: "Monthly 31st [Frank]".to_string(),
            start: NaiveDate::from_ymd_opt(2023, 1, 31)
                .unwrap()
                .and_hms_opt(9, 0, 0)
                .unwrap(),
            end: NaiveDate::from_ymd_opt(2023, 1, 31)
                .unwrap()
                .and_hms_opt(10, 0, 0)
                .unwrap(),
            uid: "uid1".to_string(),
            rrule: Some("FREQ=MONTHLY;UNTIL=20230430T235959".to_string()),
            exdates: vec![],
            recurrence_id: None,
            location: None,
            categories: vec![],
            status: None,
            source_file: None,
        };
        let expanded = expand_events(vec![monthly_31st]);
        // 4 months: Jan 31, Feb 28 (clamped), Mar 31, Apr 30 (clamped)
        assert_eq!(expanded.len(), 4);
        assert_eq!(
            expanded[0].start.date(),
            NaiveDate::from_ymd_opt(2023, 1, 31).unwrap()
        );
        assert_eq!(
            expanded[1].start.date(),
            NaiveDate::from_ymd_opt(2023, 2, 28).unwrap()
        ); // 31st -> 28th
        assert_eq!(
            expanded[2].start.date(),
            NaiveDate::from_ymd_opt(2023, 3, 31).unwrap()
        );
        assert_eq!(
            expanded[3].start.date(),
            NaiveDate::from_ymd_opt(2023, 4, 30).unwrap()
        ); // 31st -> 30th
    }

    #[test]
    fn test_expand_events_monthly_by_monthday() {
        // Test BYMONTHDAY: every 15th of the month
        let monthly_15th = RawEvent {
            summary: "Monthly 15th [Alice]".to_string(),
            start: NaiveDate::from_ymd_opt(2024, 1, 15)
                .unwrap()
                .and_hms_opt(9, 0, 0)
                .unwrap(),
            end: NaiveDate::from_ymd_opt(2024, 1, 15)
                .unwrap()
                .and_hms_opt(10, 0, 0)
                .unwrap(),
            uid: "uid1".to_string(),
            rrule: Some("FREQ=MONTHLY;BYMONTHDAY=15;UNTIL=20240615T235959".to_string()),
            exdates: vec![],
            recurrence_id: None,
            location: None,
            categories: vec![],
            status: None,
            source_file: None,
        };
        let expanded = expand_events(vec![monthly_15th]);
        // 6 months: Jan 15, Feb 15, Mar 15, Apr 15, May 15, Jun 15
        assert_eq!(expanded.len(), 6);
        assert_eq!(
            expanded[0].start.date(),
            NaiveDate::from_ymd_opt(2024, 1, 15).unwrap()
        );
        assert_eq!(
            expanded[1].start.date(),
            NaiveDate::from_ymd_opt(2024, 2, 15).unwrap()
        );
        assert_eq!(
            expanded[2].start.date(),
            NaiveDate::from_ymd_opt(2024, 3, 15).unwrap()
        );
        assert_eq!(
            expanded[3].start.date(),
            NaiveDate::from_ymd_opt(2024, 4, 15).unwrap()
        );
        assert_eq!(
            expanded[4].start.date(),
            NaiveDate::from_ymd_opt(2024, 5, 15).unwrap()
        );
        assert_eq!(
            expanded[5].start.date(),
            NaiveDate::from_ymd_opt(2024, 6, 15).unwrap()
        );
    }

    #[test]
    fn test_expand_events_monthly_by_monthday_last_day() {
        // Test BYMONTHDAY=-1 (last day of month)
        let monthly_last = RawEvent {
            summary: "Monthly last day [Bob]".to_string(),
            start: NaiveDate::from_ymd_opt(2024, 1, 31)
                .unwrap()
                .and_hms_opt(9, 0, 0)
                .unwrap(),
            end: NaiveDate::from_ymd_opt(2024, 1, 31)
                .unwrap()
                .and_hms_opt(10, 0, 0)
                .unwrap(),
            uid: "uid1".to_string(),
            rrule: Some("FREQ=MONTHLY;BYMONTHDAY=-1;UNTIL=20240630T235959".to_string()),
            exdates: vec![],
            recurrence_id: None,
            location: None,
            categories: vec![],
            status: None,
            source_file: None,
        };
        let expanded = expand_events(vec![monthly_last]);
        // Last days: Jan 31, Feb 29 (leap year 2024), Mar 31, Apr 30, May 31, Jun 30
        assert_eq!(expanded.len(), 6);
        assert_eq!(
            expanded[0].start.date(),
            NaiveDate::from_ymd_opt(2024, 1, 31).unwrap()
        );
        assert_eq!(
            expanded[1].start.date(),
            NaiveDate::from_ymd_opt(2024, 2, 29).unwrap()
        ); // Feb 29 in leap year
        assert_eq!(
            expanded[2].start.date(),
            NaiveDate::from_ymd_opt(2024, 3, 31).unwrap()
        );
        assert_eq!(
            expanded[3].start.date(),
            NaiveDate::from_ymd_opt(2024, 4, 30).unwrap()
        );
        assert_eq!(
            expanded[4].start.date(),
            NaiveDate::from_ymd_opt(2024, 5, 31).unwrap()
        );
        assert_eq!(
            expanded[5].start.date(),
            NaiveDate::from_ymd_opt(2024, 6, 30).unwrap()
        );
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

    // Note: validate_duration_range cannot be tested in isolation as it operates
    // on parsed Duration values, not raw strings. Duration validation happens
    // during CLI argument parsing via parse_human_duration / parse_duration.

    #[test]
    fn test_matches_month_filter() {
        let event = Event::new(
            "Test [Event]".to_string(),
            NaiveDate::from_ymd_opt(2024, 3, 15)
                .unwrap()
                .and_hms_opt(9, 0, 0)
                .unwrap(),
            NaiveDate::from_ymd_opt(2024, 3, 15)
                .unwrap()
                .and_hms_opt(10, 0, 0)
                .unwrap(),
        );

        assert!(matches_month_filter(&event, &None));
        assert!(matches_month_filter(&event, &Some(3)));
        assert!(!matches_month_filter(&event, &Some(1)));
        assert!(!matches_month_filter(&event, &Some(12)));
    }

    #[test]
    fn test_parse_week_filter() {
        // Test "W10" format (current year)
        let result = parse_week_filter("W10");
        assert!(result.is_some());
        let (_, week) = result.unwrap();
        assert_eq!(week, 10);

        // Test "w10" lowercase
        let result = parse_week_filter("w10");
        assert!(result.is_some());
        let (_, week) = result.unwrap();
        assert_eq!(week, 10);

        // Test bare number
        let result = parse_week_filter("25");
        assert!(result.is_some());
        let (_, week) = result.unwrap();
        assert_eq!(week, 25);

        // Test "2024-W10" format
        let result = parse_week_filter("2024-W10");
        assert!(result.is_some());
        let (year, week) = result.unwrap();
        assert_eq!(year, 2024);
        assert_eq!(week, 10);

        // Test invalid
        assert!(parse_week_filter("invalid").is_none());
        assert!(parse_week_filter("").is_none());
        assert!(parse_week_filter("W0").is_none()); // Week 0 doesn't exist
        assert!(parse_week_filter("W54").is_none()); // Week 54 doesn't exist
    }

    #[test]
    fn test_matches_week_number_filter() {
        let event = Event::new(
            "Test [Event]".to_string(),
            NaiveDate::from_ymd_opt(2024, 3, 15)
                .unwrap()
                .and_hms_opt(9, 0, 0)
                .unwrap(), // ISO week 11
            NaiveDate::from_ymd_opt(2024, 3, 15)
                .unwrap()
                .and_hms_opt(10, 0, 0)
                .unwrap(),
        );

        // No filter = all match
        assert!(matches_week_number_filter(&event, &None));

        // Wrong week
        assert!(!matches_week_number_filter(
            &event,
            &Some("W10".to_string())
        ));
        assert!(!matches_week_number_filter(
            &event,
            &Some("W12".to_string())
        ));

        // Correct week
        assert!(matches_week_number_filter(&event, &Some("W11".to_string())));

        // With year
        assert!(matches_week_number_filter(
            &event,
            &Some("2024-W11".to_string())
        ));
        assert!(!matches_week_number_filter(
            &event,
            &Some("2023-W11".to_string())
        ));

        // Invalid filter string
        assert!(!matches_week_number_filter(
            &event,
            &Some("invalid".to_string())
        ));
    }

    #[test]
    fn test_week_number_iso() {
        // ISO week numbers - chrono handles these correctly
        let date = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
        assert_eq!(date.iso_week().week(), 1); // Jan 1, 2024 is week 1

        let date = NaiveDate::from_ymd_opt(2024, 3, 15).unwrap();
        assert_eq!(date.iso_week().week(), 11); // March 15, 2024

        let date = NaiveDate::from_ymd_opt(2024, 12, 30).unwrap();
        assert_eq!(date.iso_week().week(), 1); // Dec 30, 2024 is week 1 of 2025

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
            NaiveDate::from_ymd_opt(2024, 3, 6)
                .unwrap()
                .and_hms_opt(9, 0, 0)
                .unwrap(),
            NaiveDate::from_ymd_opt(2024, 3, 6)
                .unwrap()
                .and_hms_opt(10, 0, 0)
                .unwrap(),
        );
        // Friday March 8, 2024
        let friday = Event::new(
            "Friday meeting [Bob]".to_string(),
            NaiveDate::from_ymd_opt(2024, 3, 8)
                .unwrap()
                .and_hms_opt(9, 0, 0)
                .unwrap(),
            NaiveDate::from_ymd_opt(2024, 3, 8)
                .unwrap()
                .and_hms_opt(10, 0, 0)
                .unwrap(),
        );

        // Empty filter includes all
        assert!(matches_weekday_filter(&wednesday, &[]));
        assert!(matches_weekday_filter(&friday, &[]));

        // Single day filter
        assert!(matches_weekday_filter(&wednesday, &["WE".to_string()]));
        assert!(!matches_weekday_filter(&friday, &["WE".to_string()]));

        // Multiple days filter (OR logic)
        assert!(matches_weekday_filter(
            &wednesday,
            &["MO".to_string(), "WE".to_string(), "FR".to_string()]
        ));
        assert!(matches_weekday_filter(
            &friday,
            &["MO".to_string(), "WE".to_string(), "FR".to_string()]
        ));

        // Case insensitive filter
        assert!(matches_weekday_filter(&wednesday, &["we".to_string()]));

        // Invalid weekday in filter is skipped (valid ones still work)
        assert!(matches_weekday_filter(
            &wednesday,
            &["WE".to_string(), "XX".to_string()]
        ));
        // But XX alone doesn't match anyone
        assert!(!matches_weekday_filter(&wednesday, &["XX".to_string()]));
        assert!(!matches_weekday_filter(&friday, &["XX".to_string()]));
    }

    #[test]
    fn test_matches_exclude_weekday_filter() {
        // Wednesday March 6, 2024
        let wednesday = Event::new(
            "Wednesday meeting [Alice]".to_string(),
            NaiveDate::from_ymd_opt(2024, 3, 6)
                .unwrap()
                .and_hms_opt(9, 0, 0)
                .unwrap(),
            NaiveDate::from_ymd_opt(2024, 3, 6)
                .unwrap()
                .and_hms_opt(10, 0, 0)
                .unwrap(),
        );
        // Friday March 8, 2024
        let friday = Event::new(
            "Friday meeting [Bob]".to_string(),
            NaiveDate::from_ymd_opt(2024, 3, 8)
                .unwrap()
                .and_hms_opt(9, 0, 0)
                .unwrap(),
            NaiveDate::from_ymd_opt(2024, 3, 8)
                .unwrap()
                .and_hms_opt(10, 0, 0)
                .unwrap(),
        );

        // Empty exclude list includes all
        assert!(matches_exclude_weekday_filter(&wednesday, &[]));
        assert!(matches_exclude_weekday_filter(&friday, &[]));

        // Excluding a day the event is NOT on keeps it
        assert!(matches_exclude_weekday_filter(
            &wednesday,
            &["FR".to_string()]
        ));

        // Excluding a day the event IS on excludes it
        assert!(!matches_exclude_weekday_filter(
            &wednesday,
            &["WE".to_string()]
        ));
        assert!(!matches_exclude_weekday_filter(
            &friday,
            &["FR".to_string()]
        ));

        // Multiple exclude days (OR logic - excluded if matches ANY)
        assert!(!matches_exclude_weekday_filter(
            &wednesday,
            &["MO".to_string(), "WE".to_string(), "FR".to_string()]
        ));

        // Case insensitive
        assert!(!matches_exclude_weekday_filter(
            &wednesday,
            &["we".to_string()]
        ));

        // Invalid weekday in exclude list is skipped
        assert!(!matches_exclude_weekday_filter(
            &wednesday,
            &["WE".to_string(), "XX".to_string()]
        ));
        assert!(matches_exclude_weekday_filter(
            &wednesday,
            &["XX".to_string()]
        )); // only invalid = include
    }

    #[test]
    fn test_matches_exclude_summary_filter() {
        let event = Event::new(
            "Team standup meeting [Alice] {Project}".to_string(),
            NaiveDate::from_ymd_opt(2024, 3, 15)
                .unwrap()
                .and_hms_opt(9, 0, 0)
                .unwrap(),
            NaiveDate::from_ymd_opt(2024, 3, 15)
                .unwrap()
                .and_hms_opt(10, 0, 0)
                .unwrap(),
        );

        // Empty exclude list should include
        assert!(matches_exclude_summary_filter(&event, &[]));

        // Excluding text that appears in summary should exclude
        assert!(!matches_exclude_summary_filter(
            &event,
            &["standup".to_string()]
        ));
        assert!(!matches_exclude_summary_filter(
            &event,
            &["meeting".to_string()]
        ));
        assert!(!matches_exclude_summary_filter(
            &event,
            &["TEAM".to_string()]
        )); // case insensitive
        assert!(!matches_exclude_summary_filter(
            &event,
            &["Alice".to_string()]
        )); // partial match

        // Multiple exclude filters (any match excludes)
        assert!(!matches_exclude_summary_filter(
            &event,
            &["Alice".to_string(), "xyz".to_string()]
        ));

        // Excluding text that doesn't appear should include
        assert!(matches_exclude_summary_filter(
            &event,
            &["vacation".to_string()]
        ));
    }

    #[test]
    fn test_matches_search_filter() {
        let event = Event::new(
            "Team standup meeting [Alice] {Project}".to_string(),
            NaiveDate::from_ymd_opt(2024, 3, 15)
                .unwrap()
                .and_hms_opt(9, 0, 0)
                .unwrap(),
            NaiveDate::from_ymd_opt(2024, 3, 15)
                .unwrap()
                .and_hms_opt(10, 0, 0)
                .unwrap(),
        );

        // Empty search list matches everything
        assert!(matches_search_filter(&event, &[]));

        // Single term matching
        assert!(matches_search_filter(&event, &["standup".to_string()]));
        assert!(matches_search_filter(&event, &["meeting".to_string()]));
        assert!(matches_search_filter(&event, &["TEAM".to_string()])); // case insensitive
        assert!(matches_search_filter(&event, &["Alice".to_string()])); // partial match

        // Multiple terms (AND logic - all must match)
        assert!(matches_search_filter(
            &event,
            &["standup".to_string(), "Alice".to_string()]
        ));
        assert!(matches_search_filter(
            &event,
            &["team".to_string(), "meeting".to_string()]
        ));

        // Multiple terms but one doesn't match
        assert!(!matches_search_filter(
            &event,
            &["standup".to_string(), "vacation".to_string()]
        ));

        // Term that doesn't appear at all
        assert!(!matches_search_filter(&event, &["vacation".to_string()]));

        // Case insensitive matching
        assert!(matches_search_filter(
            &event,
            &["standup meeting".to_string()]
        ));
        assert!(matches_search_filter(
            &event,
            &["TEAM".to_string(), "meeting".to_string()]
        ));
    }

    #[test]
    fn test_expand_events_yearly_recurrence() {
        let yearly = RawEvent {
            summary: "Yearly [Eve]".to_string(),
            start: NaiveDate::from_ymd_opt(2022, 6, 15)
                .unwrap()
                .and_hms_opt(9, 0, 0)
                .unwrap(),
            end: NaiveDate::from_ymd_opt(2022, 6, 15)
                .unwrap()
                .and_hms_opt(10, 0, 0)
                .unwrap(),
            uid: "uid1".to_string(),
            rrule: Some("FREQ=YEARLY;UNTIL=20251231T235959".to_string()),
            exdates: vec![],
            recurrence_id: None,
            location: None,
            categories: vec![],
            status: None,
            source_file: None,
        };
        let expanded = expand_events(vec![yearly]);
        // 4 years: 2022, 2023, 2024, 2025
        assert_eq!(expanded.len(), 4);
        assert_eq!(
            expanded[0].start.date(),
            NaiveDate::from_ymd_opt(2022, 6, 15).unwrap()
        );
        assert_eq!(
            expanded[1].start.date(),
            NaiveDate::from_ymd_opt(2023, 6, 15).unwrap()
        );
        assert_eq!(
            expanded[2].start.date(),
            NaiveDate::from_ymd_opt(2024, 6, 15).unwrap()
        );
        assert_eq!(
            expanded[3].start.date(),
            NaiveDate::from_ymd_opt(2025, 6, 15).unwrap()
        );
    }

    #[test]
    fn test_expand_events_yearly_leap_day() {
        // Feb 29 on leap years - clamped to Feb 28 on non-leap years
        let leap_day = RawEvent {
            summary: "Leap day meeting [Frank]".to_string(),
            start: NaiveDate::from_ymd_opt(2020, 2, 29)
                .unwrap()
                .and_hms_opt(9, 0, 0)
                .unwrap(),
            end: NaiveDate::from_ymd_opt(2020, 2, 29)
                .unwrap()
                .and_hms_opt(10, 0, 0)
                .unwrap(),
            uid: "uid1".to_string(),
            rrule: Some("FREQ=YEARLY;UNTIL=20251231T235959".to_string()),
            exdates: vec![],
            recurrence_id: None,
            location: None,
            categories: vec![],
            status: None,
            source_file: None,
        };
        let expanded = expand_events(vec![leap_day]);
        // Feb 29 gets clamped to Feb 28 in non-leap years
        // Limited by 5-year recurrence limit: 2020-2024 = 5 occurrences
        assert_eq!(expanded.len(), 5);
        assert_eq!(
            expanded[0].start.date(),
            NaiveDate::from_ymd_opt(2020, 2, 29).unwrap()
        );
        assert_eq!(
            expanded[1].start.date(),
            NaiveDate::from_ymd_opt(2021, 2, 28).unwrap()
        );
        assert_eq!(
            expanded[2].start.date(),
            NaiveDate::from_ymd_opt(2022, 2, 28).unwrap()
        );
        assert_eq!(
            expanded[3].start.date(),
            NaiveDate::from_ymd_opt(2023, 2, 28).unwrap()
        );
        assert_eq!(
            expanded[4].start.date(),
            NaiveDate::from_ymd_opt(2024, 2, 29).unwrap()
        );
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
        assert_eq!(
            parse_human_duration("1h30m"),
            Some(Duration::hours(1) + Duration::minutes(30))
        );
        assert_eq!(
            parse_human_duration("2h15m"),
            Some(Duration::hours(2) + Duration::minutes(15))
        );

        // Days
        assert_eq!(parse_human_duration("1d"), Some(Duration::days(1)));
        assert_eq!(parse_human_duration("2d"), Some(Duration::days(2)));

        // Weeks
        assert_eq!(parse_human_duration("1w"), Some(Duration::weeks(1)));

        // Trailing number (treated as minutes)
        assert_eq!(parse_human_duration("45"), Some(Duration::minutes(45)));

        // With spaces
        assert_eq!(
            parse_human_duration("1h 30m"),
            Some(Duration::hours(1) + Duration::minutes(30))
        );

        // Invalid
        assert_eq!(parse_human_duration(""), None);
        assert_eq!(parse_human_duration("xyz"), None);
        assert_eq!(parse_human_duration("1x"), None);
    }

    #[test]
    fn test_matches_duration_filter() {
        let short_event = Event::new(
            "Quick sync [Alice]".to_string(),
            NaiveDate::from_ymd_opt(2024, 3, 15)
                .unwrap()
                .and_hms_opt(9, 0, 0)
                .unwrap(),
            NaiveDate::from_ymd_opt(2024, 3, 15)
                .unwrap()
                .and_hms_opt(9, 15, 0)
                .unwrap(), // 15 min
        );
        let medium_event = Event::new(
            "Standard meeting [Bob]".to_string(),
            NaiveDate::from_ymd_opt(2024, 3, 15)
                .unwrap()
                .and_hms_opt(10, 0, 0)
                .unwrap(),
            NaiveDate::from_ymd_opt(2024, 3, 15)
                .unwrap()
                .and_hms_opt(11, 0, 0)
                .unwrap(), // 1 hour
        );
        let long_event = Event::new(
            "Workshop [Charlie]".to_string(),
            NaiveDate::from_ymd_opt(2024, 3, 15)
                .unwrap()
                .and_hms_opt(13, 0, 0)
                .unwrap(),
            NaiveDate::from_ymd_opt(2024, 3, 15)
                .unwrap()
                .and_hms_opt(17, 0, 0)
                .unwrap(), // 4 hours
        );

        let none_filter: Option<Duration> = None;

        // No filter = all pass
        assert!(matches_duration_filter(
            &short_event,
            &none_filter,
            &none_filter
        ));
        assert!(matches_duration_filter(
            &medium_event,
            &none_filter,
            &none_filter
        ));
        assert!(matches_duration_filter(
            &long_event,
            &none_filter,
            &none_filter
        ));

        // Min duration filter
        let min_1h = Some(Duration::hours(1));
        assert!(!matches_duration_filter(
            &short_event,
            &min_1h,
            &none_filter
        )); // 15min < 1h
        assert!(matches_duration_filter(
            &medium_event,
            &min_1h,
            &none_filter
        )); // 1h >= 1h
        assert!(matches_duration_filter(&long_event, &min_1h, &none_filter)); // 4h >= 1h

        // Max duration filter
        let max_2h = Some(Duration::hours(2));
        assert!(matches_duration_filter(&short_event, &none_filter, &max_2h)); // 15min <= 2h
        assert!(matches_duration_filter(
            &medium_event,
            &none_filter,
            &max_2h
        )); // 1h <= 2h
        assert!(!matches_duration_filter(&long_event, &none_filter, &max_2h)); // 4h > 2h

        // Combined min and max
        let min_30m = Some(Duration::minutes(30));
        let max_3h = Some(Duration::hours(3));
        assert!(!matches_duration_filter(&short_event, &min_30m, &max_3h)); // 15min < 30m
        assert!(matches_duration_filter(&medium_event, &min_30m, &max_3h)); // 1h in range
        assert!(!matches_duration_filter(&long_event, &min_30m, &max_3h)); // 4h > 3h
    }

    #[test]
    fn test_group_by_project() {
        use super::group_by_project;

        let events = vec![
            Event::new(
                "Meeting {Alpha}".to_string(),
                NaiveDate::from_ymd_opt(2024, 3, 15)
                    .unwrap()
                    .and_hms_opt(9, 0, 0)
                    .unwrap(),
                NaiveDate::from_ymd_opt(2024, 3, 15)
                    .unwrap()
                    .and_hms_opt(10, 0, 0)
                    .unwrap(),
            ),
            Event::new(
                "Standup {Alpha}".to_string(),
                NaiveDate::from_ymd_opt(2024, 3, 16)
                    .unwrap()
                    .and_hms_opt(9, 0, 0)
                    .unwrap(),
                NaiveDate::from_ymd_opt(2024, 3, 16)
                    .unwrap()
                    .and_hms_opt(10, 0, 0)
                    .unwrap(),
            ),
            Event::new(
                "Workshop {Beta}".to_string(),
                NaiveDate::from_ymd_opt(2024, 3, 17)
                    .unwrap()
                    .and_hms_opt(9, 0, 0)
                    .unwrap(),
                NaiveDate::from_ymd_opt(2024, 3, 17)
                    .unwrap()
                    .and_hms_opt(10, 0, 0)
                    .unwrap(),
            ),
            Event::new(
                "No project meeting".to_string(),
                NaiveDate::from_ymd_opt(2024, 3, 18)
                    .unwrap()
                    .and_hms_opt(9, 0, 0)
                    .unwrap(),
                NaiveDate::from_ymd_opt(2024, 3, 18)
                    .unwrap()
                    .and_hms_opt(10, 0, 0)
                    .unwrap(),
            ),
        ];
        let event_refs: Vec<_> = events.iter().collect();
        let grouped = group_by_project(&event_refs);

        assert_eq!(grouped.len(), 3); // Alpha, Beta, (none)
        assert_eq!(grouped.get("Alpha").unwrap().len(), 2);
        assert_eq!(grouped.get("Beta").unwrap().len(), 1);
        assert_eq!(grouped.get("(none)").unwrap().len(), 1);
    }

    #[test]
    fn test_dedupe_functionality() {
        use std::collections::BTreeSet;

        // Simulate the dedupe logic
        let events = vec![
            Event::new(
                "Meeting [Alice]".to_string(),
                NaiveDate::from_ymd_opt(2024, 3, 15)
                    .unwrap()
                    .and_hms_opt(9, 0, 0)
                    .unwrap(),
                NaiveDate::from_ymd_opt(2024, 3, 15)
                    .unwrap()
                    .and_hms_opt(10, 0, 0)
                    .unwrap(),
            ),
            Event::new(
                "Duplicate Meeting [Alice]".to_string(), // Different summary
                NaiveDate::from_ymd_opt(2024, 3, 15)
                    .unwrap()
                    .and_hms_opt(9, 0, 0)
                    .unwrap(),
                NaiveDate::from_ymd_opt(2024, 3, 15)
                    .unwrap()
                    .and_hms_opt(10, 0, 0)
                    .unwrap(),
            ),
            Event::new(
                "Meeting [Alice]".to_string(), // Exact duplicate
                NaiveDate::from_ymd_opt(2024, 3, 15)
                    .unwrap()
                    .and_hms_opt(9, 0, 0)
                    .unwrap(),
                NaiveDate::from_ymd_opt(2024, 3, 15)
                    .unwrap()
                    .and_hms_opt(10, 0, 0)
                    .unwrap(),
            ),
            Event::new(
                "Meeting [Alice]".to_string(), // Duplicate with different time
                NaiveDate::from_ymd_opt(2024, 3, 15)
                    .unwrap()
                    .and_hms_opt(10, 0, 0)
                    .unwrap(),
                NaiveDate::from_ymd_opt(2024, 3, 15)
                    .unwrap()
                    .and_hms_opt(11, 0, 0)
                    .unwrap(),
            ),
        ];

        let mut unique: BTreeSet<(String, chrono::NaiveDateTime, chrono::NaiveDateTime)> =
            BTreeSet::new();
        let deduped: Vec<_> = events
            .into_iter()
            .filter(|e| unique.insert((e.summary.clone(), e.start, e.end)))
            .collect();

        // Should have 3 unique events (4 originals - 1 exact duplicate)
        assert_eq!(deduped.len(), 3);
    }

    #[test]
    fn test_matches_include_recurring_filter() {
        let recurring_event = Event::with_recurring(
            "Weekly meeting [Alice]".to_string(),
            NaiveDate::from_ymd_opt(2024, 3, 15)
                .unwrap()
                .and_hms_opt(9, 0, 0)
                .unwrap(),
            NaiveDate::from_ymd_opt(2024, 3, 15)
                .unwrap()
                .and_hms_opt(10, 0, 0)
                .unwrap(),
            None,
            vec![],
            true, // is_recurring = true
        );
        let non_recurring_event = Event::new(
            "One-time meeting [Bob]".to_string(),
            NaiveDate::from_ymd_opt(2024, 3, 15)
                .unwrap()
                .and_hms_opt(11, 0, 0)
                .unwrap(),
            NaiveDate::from_ymd_opt(2024, 3, 15)
                .unwrap()
                .and_hms_opt(12, 0, 0)
                .unwrap(),
        );

        // No filter = all pass
        assert!(matches_include_recurring_filter(&recurring_event, false));
        assert!(matches_include_recurring_filter(
            &non_recurring_event,
            false
        ));

        // Include recurring filter: only pass recurring events
        assert!(matches_include_recurring_filter(&recurring_event, true));
        assert!(!matches_include_recurring_filter(
            &non_recurring_event,
            true
        ));
    }

    #[test]
    fn test_validate_time_filter_valid() {
        assert!(validate_time_filter("00:00", "start-after").is_ok());
        assert!(validate_time_filter("09:00", "start-before").is_ok());
        assert!(validate_time_filter("12:30", "end-after").is_ok());
        assert!(validate_time_filter("23:59", "end-before").is_ok());
        assert!(validate_time_filter("9:5", "test").is_ok()); // single digit
    }

    #[test]
    fn test_validate_time_filter_invalid() {
        // Invalid format (non-parseable)
        let err = validate_time_filter("invalid", "start-after").unwrap_err();
        assert!(err.to_string().contains("HH:MM"), "error: {}", err);

        // Invalid hours (> 23) - parse_time returns None, so format error
        let err = validate_time_filter("25:00", "start-after").unwrap_err();
        assert!(err.to_string().contains("HH:MM"), "error: {}", err);

        // Invalid minutes (>= 60) - parse_time returns None, so format error
        let err = validate_time_filter("09:60", "start-before").unwrap_err();
        assert!(err.to_string().contains("HH:MM"), "error: {}", err);

        // Missing parts
        let err = validate_time_filter("09", "end-after").unwrap_err();
        assert!(err.to_string().contains("HH:MM"), "error: {}", err);
    }

    #[test]
    fn test_parse_time() {
        assert_eq!(parse_time("09:00"), Some((9, 0)));
        assert_eq!(parse_time("17:30"), Some((17, 30)));
        assert_eq!(parse_time("00:00"), Some((0, 0)));
        assert_eq!(parse_time("23:59"), Some((23, 59)));
        assert_eq!(parse_time("9:00"), Some((9, 0))); // single digit hour
        assert_eq!(parse_time("9:5"), Some((9, 5))); // single digit minute
        assert_eq!(parse_time("25:00"), None); // invalid hour
        assert_eq!(parse_time("09:60"), None); // invalid minute
        assert_eq!(parse_time("9"), None); // missing colon
        assert_eq!(parse_time(""), None);
        assert_eq!(parse_time("09:00:00"), None); // extra part
    }

    #[test]
    fn test_matches_include_summary_filter() {
        let event = Event::new(
            "Team standup meeting [Alice] {Project}".to_string(),
            NaiveDate::from_ymd_opt(2024, 3, 15)
                .unwrap()
                .and_hms_opt(9, 0, 0)
                .unwrap(),
            NaiveDate::from_ymd_opt(2024, 3, 15)
                .unwrap()
                .and_hms_opt(10, 0, 0)
                .unwrap(),
        );

        // Empty list matches everything
        assert!(matches_include_summary_filter(&event, &[]));

        // Single term matching
        assert!(matches_include_summary_filter(
            &event,
            &["standup".to_string()]
        ));
        assert!(matches_include_summary_filter(
            &event,
            &["meeting".to_string()]
        ));
        assert!(matches_include_summary_filter(
            &event,
            &["TEAM".to_string()]
        )); // case insensitive

        // Multiple terms (OR logic - any match passes)
        assert!(matches_include_summary_filter(
            &event,
            &["standup".to_string(), "vacation".to_string()]
        ));
        assert!(matches_include_summary_filter(
            &event,
            &["Alice".to_string(), "Bob".to_string()]
        ));

        // No match
        assert!(!matches_include_summary_filter(
            &event,
            &["vacation".to_string()]
        ));
        assert!(!matches_include_summary_filter(
            &event,
            &["xyz".to_string()]
        ));
    }

    #[test]
    fn test_matches_start_after_filter() {
        let event = Event::new(
            "Morning meeting [Alice]".to_string(),
            NaiveDate::from_ymd_opt(2024, 3, 15)
                .unwrap()
                .and_hms_opt(9, 30, 0)
                .unwrap(),
            NaiveDate::from_ymd_opt(2024, 3, 15)
                .unwrap()
                .and_hms_opt(10, 30, 0)
                .unwrap(),
        );

        // No filter = all pass
        assert!(matches_start_after_filter(&event, &None));

        // Start after 09:00 - event at 09:30 passes
        assert!(matches_start_after_filter(
            &event,
            &Some("09:00".to_string())
        ));

        // Start after 10:00 - event at 09:30 fails
        assert!(!matches_start_after_filter(
            &event,
            &Some("10:00".to_string())
        ));

        // Start after 09:30 exactly - passes (>=)
        assert!(matches_start_after_filter(
            &event,
            &Some("09:30".to_string())
        ));

        // Start after 09:31 - fails
        assert!(!matches_start_after_filter(
            &event,
            &Some("09:31".to_string())
        ));
    }

    #[test]
    fn test_matches_start_before_filter() {
        let event = Event::new(
            "Morning meeting [Alice]".to_string(),
            NaiveDate::from_ymd_opt(2024, 3, 15)
                .unwrap()
                .and_hms_opt(9, 30, 0)
                .unwrap(),
            NaiveDate::from_ymd_opt(2024, 3, 15)
                .unwrap()
                .and_hms_opt(10, 30, 0)
                .unwrap(),
        );

        // No filter = all pass
        assert!(matches_start_before_filter(&event, &None));

        // Start before 10:00 - event at 09:30 passes
        assert!(matches_start_before_filter(
            &event,
            &Some("10:00".to_string())
        ));

        // Start before 09:00 - event at 09:30 fails
        assert!(!matches_start_before_filter(
            &event,
            &Some("09:00".to_string())
        ));

        // Start before 09:30 exactly - passes (<=)
        assert!(matches_start_before_filter(
            &event,
            &Some("09:30".to_string())
        ));

        // Start before 09:29 - fails
        assert!(!matches_start_before_filter(
            &event,
            &Some("09:29".to_string())
        ));
    }

    #[test]
    fn test_matches_end_after_filter() {
        let event = Event::new(
            "Morning meeting [Alice]".to_string(),
            NaiveDate::from_ymd_opt(2024, 3, 15)
                .unwrap()
                .and_hms_opt(9, 30, 0)
                .unwrap(),
            NaiveDate::from_ymd_opt(2024, 3, 15)
                .unwrap()
                .and_hms_opt(10, 30, 0)
                .unwrap(),
        );

        // No filter = all pass
        assert!(matches_end_after_filter(&event, &None));

        // End after 10:00 - event ends at 10:30 passes
        assert!(matches_end_after_filter(&event, &Some("10:00".to_string())));

        // End after 11:00 - event ends at 10:30 fails
        assert!(!matches_end_after_filter(
            &event,
            &Some("11:00".to_string())
        ));

        // End after 10:30 exactly - passes
        assert!(matches_end_after_filter(&event, &Some("10:30".to_string())));
    }

    #[test]
    fn test_matches_end_before_filter() {
        let event = Event::new(
            "Morning meeting [Alice]".to_string(),
            NaiveDate::from_ymd_opt(2024, 3, 15)
                .unwrap()
                .and_hms_opt(9, 30, 0)
                .unwrap(),
            NaiveDate::from_ymd_opt(2024, 3, 15)
                .unwrap()
                .and_hms_opt(10, 30, 0)
                .unwrap(),
        );

        // No filter = all pass
        assert!(matches_end_before_filter(&event, &None));

        // End before 11:00 - event ends at 10:30 passes
        assert!(matches_end_before_filter(
            &event,
            &Some("11:00".to_string())
        ));

        // End before 10:00 - event ends at 10:30 fails
        assert!(!matches_end_before_filter(
            &event,
            &Some("10:00".to_string())
        ));

        // End before 10:30 exactly - passes
        assert!(matches_end_before_filter(
            &event,
            &Some("10:30".to_string())
        ));
    }
}
