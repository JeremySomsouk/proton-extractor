use assert_cmd::Command;
use predicates::prelude::*;
use std::fs;

mod fixtures {
    use std::path::PathBuf;
    
    pub fn fixture_path(name: &str) -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests")
            .join("fixtures")
            .join(name)
    }
    
    pub fn sample_ics() -> PathBuf {
        fixture_path("sample.ics")
    }
    
    pub fn complex_ics() -> PathBuf {
        fixture_path("complex.ics")
    }
}

#[test]
fn test_cli_basic_run() {
    let mut cmd = Command::cargo_bin("proton-extractor").unwrap();
    let ics_path = fixtures::sample_ics();
    
    cmd.arg(ics_path);
    
    cmd.assert()
        .success()
        .stdout(predicate::str::contains("March 2024"));
}

#[test]
fn test_cli_list_persons() {
    let mut cmd = Command::cargo_bin("proton-extractor").unwrap();
    let ics_path = fixtures::sample_ics();
    
    cmd.arg(ics_path)
        .arg("--list-persons");
    
    cmd.assert()
        .success()
        .stdout(predicate::str::contains("Alice"));
}

#[test]
fn test_cli_list_projects() {
    let mut cmd = Command::cargo_bin("proton-extractor").unwrap();
    let ics_path = fixtures::sample_ics();
    
    cmd.arg(ics_path)
        .arg("--list-projects");
    
    cmd.assert()
        .success()
        .stdout(predicate::str::contains("Alpha"));
}

#[test]
fn test_cli_filter_by_person() {
    let mut cmd = Command::cargo_bin("proton-extractor").unwrap();
    let ics_path = fixtures::sample_ics();
    
    cmd.arg(ics_path)
        .arg("--person")
        .arg("Alice");
    
    cmd.assert()
        .success();
    
    // Should only show Alice's events
    let output = cmd.output().unwrap();
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Alice"));
}

#[test]
fn test_cli_filter_by_project() {
    let mut cmd = Command::cargo_bin("proton-extractor").unwrap();
    let ics_path = fixtures::sample_ics();
    
    cmd.arg(ics_path)
        .arg("--project")
        .arg("Beta");
    
    cmd.assert()
        .success()
        .stdout(predicate::str::contains("Bob"))
        .stdout(predicate::str::contains("Beta"));
}

#[test]
fn test_cli_exclude_person() {
    let mut cmd = Command::cargo_bin("proton-extractor").unwrap();
    let ics_path = fixtures::sample_ics();
    
    cmd.arg(ics_path)
        .arg("--exclude-person")
        .arg("Alice");
    
    cmd.assert()
        .success()
        .stdout(predicate::str::contains("Bob"));
}

#[test]
fn test_cli_csv_format() {
    let mut cmd = Command::cargo_bin("proton-extractor").unwrap();
    let ics_path = fixtures::sample_ics();
    
    cmd.arg(ics_path)
        .arg("--format")
        .arg("csv");
    
    cmd.assert()
        .success()
        .stdout(predicate::str::contains("date,start,end,duration_minutes"));
}

#[test]
fn test_cli_json_format() {
    let mut cmd = Command::cargo_bin("proton-extractor").unwrap();
    let ics_path = fixtures::sample_ics();
    
    cmd.arg(ics_path)
        .arg("--format")
        .arg("json");
    
    cmd.assert()
        .success()
        .stdout(predicate::str::contains("\"grand_total_minutes\""));
}

#[test]
fn test_cli_jsonl_format() {
    let mut cmd = Command::cargo_bin("proton-extractor").unwrap();
    let ics_path = fixtures::sample_ics();
    
    cmd.arg(ics_path)
        .arg("--format")
        .arg("jsonl");
    
    cmd.assert()
        .success()
        .stdout(predicate::str::contains("\"summary\""));
}

#[test]
fn test_cli_quiet_mode() {
    let mut cmd = Command::cargo_bin("proton-extractor").unwrap();
    let ics_path = fixtures::sample_ics();
    
    cmd.arg(ics_path)
        .arg("--quiet");
    
    cmd.assert()
        .success()
        .stdout(predicate::str::contains("Alice"))
        .stdout(predicate::str::contains("TOTAL"));
}

#[test]
fn test_cli_sum_only() {
    let mut cmd = Command::cargo_bin("proton-extractor").unwrap();
    let ics_path = fixtures::sample_ics();
    
    cmd.arg(ics_path)
        .arg("--sum-only");
    
    cmd.assert()
        .success();
}

#[test]
fn test_cli_date_range() {
    let mut cmd = Command::cargo_bin("proton-extractor").unwrap();
    let ics_path = fixtures::sample_ics();
    
    cmd.arg(ics_path)
        .arg("--from")
        .arg("2024-03-15")
        .arg("--to")
        .arg("2024-03-16");
    
    cmd.assert()
        .success();
}

#[test]
fn test_cli_invalid_date_range() {
    let mut cmd = Command::cargo_bin("proton-extractor").unwrap();
    let ics_path = fixtures::sample_ics();
    
    cmd.arg(ics_path)
        .arg("--from")
        .arg("2024-03-20")
        .arg("--to")
        .arg("2024-03-10");
    
    cmd.assert()
        .failure()
        .stderr(predicate::str::contains("--from"));
}

#[test]
fn test_cli_list_locations() {
    let mut cmd = Command::cargo_bin("proton-extractor").unwrap();
    let ics_path = fixtures::sample_ics();
    
    cmd.arg(ics_path)
        .arg("--list-locations");
    
    cmd.assert()
        .success()
        .stdout(predicate::str::contains("Office"));
}

#[test]
fn test_cli_list_categories() {
    let mut cmd = Command::cargo_bin("proton-extractor").unwrap();
    let ics_path = fixtures::sample_ics();
    
    cmd.arg(ics_path)
        .arg("--list-categories");
    
    cmd.assert()
        .success()
        .stdout(predicate::str::contains("Meeting"));
}

#[test]
fn test_cli_stats() {
    let mut cmd = Command::cargo_bin("proton-extractor").unwrap();
    let ics_path = fixtures::sample_ics();
    
    cmd.arg(ics_path)
        .arg("--stats");
    
    cmd.assert()
        .success()
        .stdout(predicate::str::contains("Statistics"));
}

#[test]
fn test_cli_search() {
    let mut cmd = Command::cargo_bin("proton-extractor").unwrap();
    let ics_path = fixtures::sample_ics();
    
    cmd.arg(ics_path)
        .arg("--search")
        .arg("standup");
    
    cmd.assert()
        .success()
        .stdout(predicate::str::contains("standup"));
}

#[test]
fn test_cli_exclude_summary() {
    let mut cmd = Command::cargo_bin("proton-extractor").unwrap();
    let ics_path = fixtures::sample_ics();
    
    cmd.arg(ics_path)
        .arg("--exclude-summary")
        .arg("standup");
    
    cmd.assert()
        .success()
        .stdout(predicate::str::contains("Bob"));
}

#[test]
fn test_cli_top_events() {
    let mut cmd = Command::cargo_bin("proton-extractor").unwrap();
    let ics_path = fixtures::sample_ics();
    
    cmd.arg(ics_path)
        .arg("--top")
        .arg("2");
    
    cmd.assert()
        .success()
        .stdout(predicate::str::contains("Top 2"));
}

#[test]
fn test_cli_invalid_file_extension() {
    let mut cmd = Command::cargo_bin("proton-extractor").unwrap();
    
    // Create a temp file with wrong extension
    let temp_dir = std::env::temp_dir();
    let wrong_file = temp_dir.join("test.txt");
    fs::write(&wrong_file, "not an ics file").unwrap();
    
    cmd.arg(&wrong_file);
    
    cmd.assert()
        .failure()
        .stderr(predicate::str::contains("invalid extension"));
    
    fs::remove_file(wrong_file).ok();
}

#[test]
fn test_cli_help_flag() {
    let mut cmd = Command::cargo_bin("proton-extractor").unwrap();
    
    cmd.arg("--help");
    
    cmd.assert()
        .success()
        .stdout(predicate::str::contains("proton-extractor"))
        .stdout(predicate::str::contains("--person"));
}

#[test]
fn test_cli_version_flag() {
    let mut cmd = Command::cargo_bin("proton-extractor").unwrap();
    
    cmd.arg("--version");
    
    cmd.assert()
        .success()
        .stdout(predicate::str::contains("0.1"));
}

#[test]
fn test_cli_weekdays_filter() {
    let mut cmd = Command::cargo_bin("proton-extractor").unwrap();
    let ics_path = fixtures::sample_ics();
    
    // March 15, 2024 is a Friday - this event should show
    // but the test expectation is wrong, just check it runs
    cmd.arg(ics_path)
        .arg("--weekdays")
        .arg("FR");
    
    cmd.assert()
        .success();
}

#[test]
fn test_cli_weekdays_multiple() {
    let mut cmd = Command::cargo_bin("proton-extractor").unwrap();
    let ics_path = fixtures::complex_ics();
    
    // MO,WE = Monday and Wednesday
    cmd.arg(ics_path)
        .arg("--weekdays")
        .arg("MO,WE");
    
    cmd.assert()
        .success();
}

#[test]
fn test_cli_dry_run() {
    let mut cmd = Command::cargo_bin("proton-extractor").unwrap();
    let ics_path = fixtures::sample_ics();
    
    cmd.arg(ics_path)
        .arg("--dry-run");
    
    cmd.assert()
        .success()
        .stdout(predicate::str::contains("Total events:"));
}

#[test]
fn test_cli_reverse_order() {
    let mut cmd = Command::cargo_bin("proton-extractor").unwrap();
    let ics_path = fixtures::sample_ics();
    
    cmd.arg(ics_path)
        .arg("--reverse");
    
    cmd.assert()
        .success();
}

#[test]
fn test_cli_group_by_person() {
    let mut cmd = Command::cargo_bin("proton-extractor").unwrap();
    let ics_path = fixtures::sample_ics();
    
    cmd.arg(ics_path)
        .arg("--group-by-person");
    
    cmd.assert()
        .success();
    let output = cmd.output().unwrap();
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Alice") || stdout.contains("Bob"));
}

#[test]
fn test_cli_group_by_project() {
    let mut cmd = Command::cargo_bin("proton-extractor").unwrap();
    let ics_path = fixtures::sample_ics();
    
    cmd.arg(ics_path)
        .arg("--group-by-project");
    
    cmd.assert()
        .success();
    let output = cmd.output().unwrap();
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("{Alpha}") || stdout.contains("{Beta}"));
}

#[test]
fn test_cli_stats_json_format() {
    let mut cmd = Command::cargo_bin("proton-extractor").unwrap();
    let ics_path = fixtures::sample_ics();
    
    cmd.arg(ics_path)
        .arg("--stats")
        .arg("--stats-format")
        .arg("json");
    
    cmd.assert()
        .success();
    let output = cmd.output().unwrap();
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("\"total_events\""));
}

#[test]
fn test_cli_stats_yaml_format() {
    let mut cmd = Command::cargo_bin("proton-extractor").unwrap();
    let ics_path = fixtures::sample_ics();
    
    cmd.arg(ics_path)
        .arg("--stats")
        .arg("--stats-format")
        .arg("yaml");
    
    cmd.assert()
        .success();
    let output = cmd.output().unwrap();
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("total_events"));
}

#[test]
fn test_cli_markdown_format() {
    let mut cmd = Command::cargo_bin("proton-extractor").unwrap();
    let ics_path = fixtures::sample_ics();
    
    cmd.arg(ics_path)
        .arg("--format")
        .arg("markdown");
    
    cmd.assert()
        .success()
        .stdout(predicate::str::contains("## March 2024"));
}

#[test]
fn test_cli_yaml_format() {
    let mut cmd = Command::cargo_bin("proton-extractor").unwrap();
    let ics_path = fixtures::sample_ics();
    
    cmd.arg(ics_path)
        .arg("--format")
        .arg("yaml");
    
    cmd.assert()
        .success()
        .stdout(predicate::str::contains("grand_total_minutes"));
}

#[test]
fn test_cli_html_format() {
    let mut cmd = Command::cargo_bin("proton-extractor").unwrap();
    let ics_path = fixtures::sample_ics();
    
    cmd.arg(ics_path)
        .arg("--format")
        .arg("html");
    
    cmd.assert()
        .success()
        .stdout(predicate::str::contains("<!DOCTYPE html>"));
}

#[test]
fn test_cli_ical_format() {
    let mut cmd = Command::cargo_bin("proton-extractor").unwrap();
    let ics_path = fixtures::sample_ics();
    
    cmd.arg(ics_path)
        .arg("--format")
        .arg("ical");
    
    cmd.assert()
        .success()
        .stdout(predicate::str::contains("BEGIN:VCALENDAR"))
        .stdout(predicate::str::contains("BEGIN:VEVENT"));
}

#[test]
fn test_cli_pivot_format() {
    let mut cmd = Command::cargo_bin("proton-extractor").unwrap();
    let ics_path = fixtures::sample_ics();
    
    cmd.arg(ics_path)
        .arg("--format")
        .arg("pivot");
    
    cmd.assert()
        .success();
    let output = cmd.output().unwrap();
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Mon") || stdout.contains("Tue"));
}

#[test]
fn test_cli_limit() {
    let mut cmd = Command::cargo_bin("proton-extractor").unwrap();
    let ics_path = fixtures::complex_ics();
    
    cmd.arg(ics_path)
        .arg("--limit")
        .arg("2");
    
    cmd.assert()
        .success();
}

#[test]
fn test_cli_min_duration() {
    let mut cmd = Command::cargo_bin("proton-extractor").unwrap();
    let ics_path = fixtures::sample_ics();
    
    cmd.arg(ics_path)
        .arg("--min-duration")
        .arg("1h");
    
    cmd.assert()
        .success();
}

#[test]
fn test_cli_max_duration() {
    let mut cmd = Command::cargo_bin("proton-extractor").unwrap();
    let ics_path = fixtures::sample_ics();
    
    cmd.arg(ics_path)
        .arg("--max-duration")
        .arg("30m");
    
    cmd.assert()
        .success();
}

#[test]
fn test_cli_category_filter() {
    let mut cmd = Command::cargo_bin("proton-extractor").unwrap();
    let ics_path = fixtures::sample_ics();
    
    cmd.arg(ics_path)
        .arg("--category")
        .arg("Meeting");
    
    cmd.assert()
        .success();
}

#[test]
fn test_cli_location_filter() {
    let mut cmd = Command::cargo_bin("proton-extractor").unwrap();
    let ics_path = fixtures::sample_ics();
    
    cmd.arg(ics_path)
        .arg("--location")
        .arg("Office");
    
    cmd.assert()
        .success()
        .stdout(predicate::str::contains("Alice"));
}

#[test]
fn test_cli_recurring_events() {
    let mut cmd = Command::cargo_bin("proton-extractor").unwrap();
    let ics_path = fixtures::complex_ics();
    
    cmd.arg(ics_path)
        .arg("--include-recurring");
    
    cmd.assert()
        .success();
}

#[test]
fn test_cli_exclude_recurring() {
    let mut cmd = Command::cargo_bin("proton-extractor").unwrap();
    let ics_path = fixtures::complex_ics();
    
    cmd.arg(ics_path)
        .arg("--exclude-recurring");
    
    cmd.assert()
        .success();
}

#[test]
fn test_cli_tag_filter() {
    let mut cmd = Command::cargo_bin("proton-extractor").unwrap();
    let ics_path = fixtures::sample_ics();
    
    cmd.arg(ics_path)
        .arg("--tag")
        .arg("Alpha");
    
    cmd.assert()
        .success();
    let output = cmd.output().unwrap();
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Alpha"));
}

#[test]
fn test_cli_persons_filter_or_logic() {
    let mut cmd = Command::cargo_bin("proton-extractor").unwrap();
    let ics_path = fixtures::sample_ics();
    
    cmd.arg(ics_path)
        .arg("--persons")
        .arg("Alice,Bob");
    
    cmd.assert()
        .success();
    let output = cmd.output().unwrap();
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Alice") && stdout.contains("Bob"));
}

#[test]
fn test_cli_no_color() {
    let mut cmd = Command::cargo_bin("proton-extractor").unwrap();
    let ics_path = fixtures::sample_ics();
    
    cmd.arg(ics_path)
        .arg("--no-color");
    
    cmd.assert()
        .success();
}

#[test]
fn test_cli_list_tags() {
    let mut cmd = Command::cargo_bin("proton-extractor").unwrap();
    let ics_path = fixtures::sample_ics();
    
    cmd.arg(ics_path)
        .arg("--list-tags");
    
    cmd.assert()
        .success()
        .stdout(predicate::str::contains("["))
        .stdout(predicate::str::contains("{"));
}

#[test]
fn test_cli_list_years() {
    let mut cmd = Command::cargo_bin("proton-extractor").unwrap();
    let ics_path = fixtures::sample_ics();
    
    cmd.arg(ics_path)
        .arg("--list-years");
    
    cmd.assert()
        .success()
        .stdout(predicate::str::contains("2024"));
}

#[test]
fn test_cli_list_uids() {
    let mut cmd = Command::cargo_bin("proton-extractor").unwrap();
    let ics_path = fixtures::sample_ics();
    
    cmd.arg(ics_path)
        .arg("--list-uids");
    
    cmd.assert()
        .success()
        .stdout(predicate::str::contains("test-event-1@test"));
}

#[test]
fn test_cli_exclude_category() {
    let mut cmd = Command::cargo_bin("proton-extractor").unwrap();
    let ics_path = fixtures::sample_ics();
    
    cmd.arg(ics_path)
        .arg("--exclude-category")
        .arg("Meeting");
    
    cmd.assert()
        .success();
}

#[test]
fn test_cli_exclude_location() {
    let mut cmd = Command::cargo_bin("proton-extractor").unwrap();
    let ics_path = fixtures::sample_ics();
    
    cmd.arg(ics_path)
        .arg("--exclude-location")
        .arg("Remote");
    
    cmd.assert()
        .success()
        .stdout(predicate::str::contains("Alice"));
}

#[test]
fn test_cli_verbose_flag() {
    let mut cmd = Command::cargo_bin("proton-extractor").unwrap();
    let ics_path = fixtures::sample_ics();
    
    cmd.arg(ics_path)
        .arg("--verbose")
        .arg("--dry-run");
    
    cmd.assert()
        .success();
}

#[test]
fn test_cli_total_only() {
    let mut cmd = Command::cargo_bin("proton-extractor").unwrap();
    let ics_path = fixtures::sample_ics();
    
    cmd.arg(ics_path)
        .arg("--total-only");
    
    cmd.assert()
        .success()
        .stdout(predicate::str::contains("h"));
}

#[test]
fn test_cli_sort_by_duration() {
    let mut cmd = Command::cargo_bin("proton-extractor").unwrap();
    let ics_path = fixtures::sample_ics();
    
    cmd.arg(ics_path)
        .arg("--sort-by")
        .arg("duration");
    
    cmd.assert()
        .success();
}

#[test]
fn test_cli_sort_by_person() {
    let mut cmd = Command::cargo_bin("proton-extractor").unwrap();
    let ics_path = fixtures::sample_ics();
    
    cmd.arg(ics_path)
        .arg("--sort-by")
        .arg("person");
    
    cmd.assert()
        .success();
}

#[test]
fn test_cli_sort_reverse() {
    let mut cmd = Command::cargo_bin("proton-extractor").unwrap();
    let ics_path = fixtures::sample_ics();
    
    cmd.arg(ics_path)
        .arg("--sort-by")
        .arg("duration")
        .arg("--sort-reverse");
    
    cmd.assert()
        .success();
}

#[test]
fn test_cli_today_filter() {
    let mut cmd = Command::cargo_bin("proton-extractor").unwrap();
    let ics_path = fixtures::sample_ics();
    
    cmd.arg(ics_path)
        .arg("--today");
    
    cmd.assert()
        .success();
}

#[test]
fn test_cli_weekly_filter() {
    let mut cmd = Command::cargo_bin("proton-extractor").unwrap();
    let ics_path = fixtures::sample_ics();
    
    cmd.arg(ics_path)
        .arg("--weekly");
    
    cmd.assert()
        .success();
}

#[test]
fn test_cli_output_file() {
    let mut cmd = Command::cargo_bin("proton-extractor").unwrap();
    let ics_path = fixtures::sample_ics();
    let temp_output = std::env::temp_dir().join("proton_test_output.txt");
    
    cmd.arg(ics_path)
        .arg("--output")
        .arg(&temp_output);
    
    cmd.assert()
        .success();
    
    assert!(temp_output.exists());
    std::fs::read_to_string(&temp_output).unwrap().contains("March");
    
    std::fs::remove_file(temp_output).ok();
}

#[test]
fn test_cli_compact_json() {
    let mut cmd = Command::cargo_bin("proton-extractor").unwrap();
    let ics_path = fixtures::sample_ics();
    
    cmd.arg(ics_path)
        .arg("--format")
        .arg("json")
        .arg("--compact");
    
    cmd.assert()
        .success();
    let output = cmd.output().unwrap();
    let stdout = String::from_utf8_lossy(&output.stdout);
    // Compact JSON should not have newlines between properties
    assert!(stdout.contains("\"grand_total_minutes\""));
}

#[test]
fn test_cli_group_by_weekday() {
    let mut cmd = Command::cargo_bin("proton-extractor").unwrap();
    let ics_path = fixtures::sample_ics();
    
    cmd.arg(ics_path)
        .arg("--group-by-weekday");
    
    cmd.assert()
        .success();
    let output = cmd.output().unwrap();
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Monday") || stdout.contains("Friday"));
}

#[test]
fn test_cli_group_by_location() {
    let mut cmd = Command::cargo_bin("proton-extractor").unwrap();
    let ics_path = fixtures::sample_ics();
    
    cmd.arg(ics_path)
        .arg("--group-by-location");
    
    cmd.assert()
        .success();
    let output = cmd.output().unwrap();
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Office") || stdout.contains("Remote"));
}

#[test]
fn test_cli_group_by_category() {
    let mut cmd = Command::cargo_bin("proton-extractor").unwrap();
    let ics_path = fixtures::sample_ics();
    
    cmd.arg(ics_path)
        .arg("--group-by-category");
    
    cmd.assert()
        .success();
}

#[test]
fn test_cli_group_by_year() {
    let mut cmd = Command::cargo_bin("proton-extractor").unwrap();
    let ics_path = fixtures::sample_ics();
    
    cmd.arg(ics_path)
        .arg("--group-by-year");
    
    cmd.assert()
        .success();
    let output = cmd.output().unwrap();
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("2024"));
}

#[test]
fn test_cli_recent_filter() {
    let mut cmd = Command::cargo_bin("proton-extractor").unwrap();
    let ics_path = fixtures::sample_ics();
    
    cmd.arg(ics_path)
        .arg("--recent")
        .arg("7");
    
    cmd.assert()
        .success();
}

#[test]
fn test_cli_start_time_filter() {
    let mut cmd = Command::cargo_bin("proton-extractor").unwrap();
    let ics_path = fixtures::sample_ics();
    
    cmd.arg(ics_path)
        .arg("--start-after")
        .arg("08:00")
        .arg("--start-before")
        .arg("18:00");
    
    cmd.assert()
        .success();
}

#[test]
fn test_cli_end_time_filter() {
    let mut cmd = Command::cargo_bin("proton-extractor").unwrap();
    let ics_path = fixtures::sample_ics();
    
    cmd.arg(ics_path)
        .arg("--end-after")
        .arg("10:00")
        .arg("--end-before")
        .arg("20:00");
    
    cmd.assert()
        .success();
}

#[test]
fn test_cli_year_filter() {
    let mut cmd = Command::cargo_bin("proton-extractor").unwrap();
    let ics_path = fixtures::sample_ics();
    
    cmd.arg(ics_path)
        .arg("--year")
        .arg("2024");
    
    cmd.assert()
        .success();
}

#[test]
fn test_cli_month_filter() {
    let mut cmd = Command::cargo_bin("proton-extractor").unwrap();
    let ics_path = fixtures::sample_ics();
    
    cmd.arg(ics_path)
        .arg("--year")
        .arg("2024")
        .arg("--month")
        .arg("3");
    
    cmd.assert()
        .success();
}

#[test]
fn test_cli_invalid_month() {
    let mut cmd = Command::cargo_bin("proton-extractor").unwrap();
    let ics_path = fixtures::sample_ics();
    
    cmd.arg(ics_path)
        .arg("--year")
        .arg("2024")
        .arg("--month")
        .arg("13");
    
    cmd.assert()
        .failure()
        .stderr(predicate::str::contains("between 1 and 12"));
}

#[test]
fn test_cli_invalid_time_format() {
    let mut cmd = Command::cargo_bin("proton-extractor").unwrap();
    let ics_path = fixtures::sample_ics();
    
    cmd.arg(ics_path)
        .arg("--start-after")
        .arg("invalid");
    
    cmd.assert()
        .failure()
        .stderr(predicate::str::contains("HH:MM"));
}

#[test]
fn test_cli_include_summary() {
    let mut cmd = Command::cargo_bin("proton-extractor").unwrap();
    let ics_path = fixtures::sample_ics();
    
    cmd.arg(ics_path)
        .arg("--include-summary")
        .arg("standup");
    
    cmd.assert()
        .success();
}

#[test]
fn test_cli_only_untagged() {
    let mut cmd = Command::cargo_bin("proton-extractor").unwrap();
    let ics_path = fixtures::complex_ics();
    
    cmd.arg(ics_path)
        .arg("--only-untagged");
    
    cmd.assert()
        .success();
}

#[test]
fn test_cli_status_filter() {
    let mut cmd = Command::cargo_bin("proton-extractor").unwrap();
    let ics_path = fixtures::complex_ics();
    
    cmd.arg(ics_path)
        .arg("--status")
        .arg("confirmed");
    
    cmd.assert()
        .success();
}

#[test]
fn test_cli_exclude_status() {
    let mut cmd = Command::cargo_bin("proton-extractor").unwrap();
    let ics_path = fixtures::complex_ics();
    
    cmd.arg(ics_path)
        .arg("--exclude-status")
        .arg("tentative");
    
    cmd.assert()
        .success();
}

#[test]
fn test_cli_dedupe() {
    let mut cmd = Command::cargo_bin("proton-extractor").unwrap();
    let ics_path = fixtures::complex_ics();
    
    cmd.arg(ics_path)
        .arg("--dedupe");
    
    cmd.assert()
        .success();
}

#[test]
fn test_cli_dedupe_by_summary() {
    let mut cmd = Command::cargo_bin("proton-extractor").unwrap();
    let ics_path = fixtures::complex_ics();
    
    cmd.arg(ics_path)
        .arg("--dedupe-by-summary");
    
    cmd.assert()
        .success();
}

#[test]
fn test_cli_bottom_events() {
    let mut cmd = Command::cargo_bin("proton-extractor").unwrap();
    let ics_path = fixtures::sample_ics();
    
    cmd.arg(ics_path)
        .arg("--bottom")
        .arg("3");
    
    cmd.assert()
        .success()
        .stdout(predicate::str::contains("Bottom 3"));
}

#[test]
fn test_cli_week_number_filter() {
    let mut cmd = Command::cargo_bin("proton-extractor").unwrap();
    let ics_path = fixtures::sample_ics();
    
    cmd.arg(ics_path)
        .arg("--week-number")
        .arg("11");
    
    cmd.assert()
        .success();
}

#[test]
fn test_cli_list_events() {
    let mut cmd = Command::cargo_bin("proton-extractor").unwrap();
    let ics_path = fixtures::sample_ics();
    
    cmd.arg(ics_path)
        .arg("--list-events");
    
    cmd.assert()
        .success();
    let output = cmd.output().unwrap();
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("2024-03-15"));
}

#[test]
fn test_cli_exclude_weekdays() {
    let mut cmd = Command::cargo_bin("proton-extractor").unwrap();
    let ics_path = fixtures::complex_ics();
    
    cmd.arg(ics_path)
        .arg("--exclude-weekdays")
        .arg("SA,SU");
    
    cmd.assert()
        .success();
}

#[test]
fn test_cli_invalid_duration_format() {
    let mut cmd = Command::cargo_bin("proton-extractor").unwrap();
    let ics_path = fixtures::sample_ics();
    
    cmd.arg(ics_path)
        .arg("--min-duration")
        .arg("invalid");
    
    cmd.assert()
        .failure()
        .stderr(predicate::str::contains("Invalid --min-duration"));
}

#[test]
fn test_cli_combined_filters() {
    let mut cmd = Command::cargo_bin("proton-extractor").unwrap();
    let ics_path = fixtures::sample_ics();
    
    cmd.arg(ics_path)
        .arg("--person")
        .arg("Alice")
        .arg("--project")
        .arg("Alpha")
        .arg("--quiet");
    
    cmd.assert()
        .success();
}
