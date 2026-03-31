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
        .stdout(predicate::str::contains("invalid extension"));
    
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
