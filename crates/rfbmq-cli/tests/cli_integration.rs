use std::path::Path;

use assert_cmd::Command;
use predicates::prelude::*;
use tempfile::TempDir;

fn rfbmq() -> Command {
    Command::cargo_bin("rfbmq").expect("binary rfbmq not found")
}

fn init_queue(dir: &Path) {
    rfbmq()
        .args(["init", dir.to_str().unwrap()])
        .assert()
        .success();
}

fn init_queue_with_args(dir: &Path, extra: &[&str]) {
    let mut cmd = rfbmq();
    cmd.arg("init").arg(dir);
    for a in extra {
        cmd.arg(a);
    }
    cmd.assert().success();
}

/// Push a message body via stdin, return the 32-char hex ID.
fn push_body(dir: &Path, body: &str) -> String {
    let out = rfbmq()
        .args(["push", dir.to_str().unwrap()])
        .write_stdin(body)
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    String::from_utf8(out).unwrap().trim().to_string()
}

/// Find the first `.md` file under `dir` (non-recursive).
fn first_md_in(dir: &Path) -> String {
    for entry in std::fs::read_dir(dir).unwrap() {
        let entry = entry.unwrap();
        let name = entry.file_name().to_string_lossy().into_owned();
        if name.ends_with(".md") {
            return entry.path().to_string_lossy().into_owned();
        }
    }
    panic!("no .md file found in {}", dir.display());
}

// ── 1. init ─────────────────────────────────────────────────────────────

#[test]
fn test_cli_init() {
    let tmp = TempDir::new().unwrap();
    let qdir = tmp.path().join("q");

    rfbmq()
        .args(["init", qdir.to_str().unwrap()])
        .assert()
        .success();

    assert!(qdir.join("pending").is_dir());
    assert!(qdir.join("processing").is_dir());
    assert!(qdir.join("done").is_dir());
    assert!(qdir.join("failed").is_dir());
    assert!(qdir.join(".tmp").is_dir());
}

// ── 2. init --priority ──────────────────────────────────────────────────

#[test]
fn test_cli_init_priority() {
    let tmp = TempDir::new().unwrap();
    let qdir = tmp.path().join("q");

    rfbmq()
        .args(["init", qdir.to_str().unwrap(), "--priority"])
        .assert()
        .success();

    assert!(qdir.join("pending/0-critical").is_dir());
    assert!(qdir.join("pending/1-high").is_dir());
    assert!(qdir.join("pending/2-normal").is_dir());
    assert!(qdir.join("pending/3-low").is_dir());
}

// ── 3. push prints 32-char hex ID ───────────────────────────────────────

#[test]
fn test_cli_push_stdout_id() {
    let tmp = TempDir::new().unwrap();
    let qdir = tmp.path().join("q");
    init_queue(&qdir);

    let id = push_body(&qdir, "hello\n");
    assert_eq!(id.len(), 32);
    assert!(id.chars().all(|c| c.is_ascii_hexdigit()));
}

// ── 4. push with options ────────────────────────────────────────────────

#[test]
fn test_cli_push_with_options() {
    let tmp = TempDir::new().unwrap();
    let qdir = tmp.path().join("q");
    init_queue(&qdir);

    let out = rfbmq()
        .args([
            "push",
            qdir.to_str().unwrap(),
            "-p", "high",
            "-t", "3600",
            "-c", "req-42",
            "-T", "urgent",
            "-T", "deploy",
            "-b", "ci-server",
        ])
        .write_stdin("payload")
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();

    let id = String::from_utf8(out).unwrap().trim().to_string();

    // Find the enqueued file and inspect it
    let msg_path = first_md_in(&qdir.join("pending"));

    rfbmq()
        .args(["inspect", &msg_path])
        .assert()
        .success()
        .stdout(predicate::str::contains(format!("Id:             {}", id)))
        .stdout(predicate::str::contains("Priority:       high"))
        .stdout(predicate::str::contains("TTL:            3600"))
        .stdout(predicate::str::contains("Tags:           urgent, deploy"))
        .stdout(predicate::str::contains("Correlation-Id: req-42"))
        .stdout(predicate::str::contains("Created-By:     ci-server"));
}

// ── 5. push from file ───────────────────────────────────────────────────

#[test]
fn test_cli_push_from_file() {
    let tmp = TempDir::new().unwrap();
    let qdir = tmp.path().join("q");
    init_queue(&qdir);

    let body_file = tmp.path().join("file.md");
    std::fs::write(&body_file, "file body content\n").unwrap();

    rfbmq()
        .args(["push", qdir.to_str().unwrap(), body_file.to_str().unwrap()])
        .assert()
        .success();

    let msg_path = first_md_in(&qdir.join("pending"));
    rfbmq()
        .args(["cat", &msg_path])
        .assert()
        .success()
        .stdout(predicate::str::contains("file body content"));
}

// ── 6. depth ────────────────────────────────────────────────────────────

#[test]
fn test_cli_depth() {
    let tmp = TempDir::new().unwrap();
    let qdir = tmp.path().join("q");
    init_queue(&qdir);

    push_body(&qdir, "a");
    push_body(&qdir, "b");
    push_body(&qdir, "c");

    rfbmq()
        .args(["depth", qdir.to_str().unwrap()])
        .assert()
        .success()
        .stdout(predicate::str::starts_with("3"));
}

// ── 7. inspect ──────────────────────────────────────────────────────────

#[test]
fn test_cli_inspect() {
    let tmp = TempDir::new().unwrap();
    let qdir = tmp.path().join("q");
    init_queue(&qdir);

    let id = push_body(&qdir, "some body");
    let msg_path = first_md_in(&qdir.join("pending"));

    rfbmq()
        .args(["inspect", &msg_path])
        .assert()
        .success()
        .stdout(predicate::str::contains(format!("Id:             {}", id)))
        .stdout(predicate::str::contains("Priority:"))
        .stdout(predicate::str::contains("Created-By:"));
}

// ── 8. cat ──────────────────────────────────────────────────────────────

#[test]
fn test_cli_cat() {
    let tmp = TempDir::new().unwrap();
    let qdir = tmp.path().join("q");
    init_queue(&qdir);

    push_body(&qdir, "the body text");
    let msg_path = first_md_in(&qdir.join("pending"));

    rfbmq()
        .args(["cat", &msg_path])
        .assert()
        .success()
        .stdout(predicate::str::contains("the body text"))
        .stdout(predicate::str::contains("Id:").not())
        .stdout(predicate::str::contains("Priority:").not());
}

// ── 9. pop ──────────────────────────────────────────────────────────────

#[test]
fn test_cli_pop() {
    let tmp = TempDir::new().unwrap();
    let qdir = tmp.path().join("q");
    init_queue(&qdir);
    push_body(&qdir, "pop-me");

    rfbmq()
        .args(["pop", qdir.to_str().unwrap()])
        .assert()
        .success()
        .stdout(predicate::str::contains("processing"));
}

// ── 10. pop empty ───────────────────────────────────────────────────────

#[test]
fn test_cli_pop_empty() {
    let tmp = TempDir::new().unwrap();
    let qdir = tmp.path().join("q");
    init_queue(&qdir);

    rfbmq()
        .args(["pop", qdir.to_str().unwrap()])
        .assert()
        .code(1);
}

// ── 11. ack ─────────────────────────────────────────────────────────────

#[test]
fn test_cli_ack() {
    let tmp = TempDir::new().unwrap();
    let qdir = tmp.path().join("q");
    init_queue(&qdir);
    push_body(&qdir, "ack-me");

    // Pop to get claimed path
    let out = rfbmq()
        .args(["pop", qdir.to_str().unwrap()])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let claimed = String::from_utf8(out).unwrap().trim().to_string();

    rfbmq()
        .args(["ack", qdir.to_str().unwrap(), &claimed])
        .assert()
        .success();

    // The file should now be in done/
    assert!(std::fs::read_dir(qdir.join("done"))
        .unwrap()
        .any(|e| e.unwrap().file_name().to_string_lossy().ends_with(".md")));
}

// ── 12. nack ────────────────────────────────────────────────────────────

#[test]
fn test_cli_nack() {
    let tmp = TempDir::new().unwrap();
    let qdir = tmp.path().join("q");
    init_queue(&qdir);
    push_body(&qdir, "nack-me");

    let out = rfbmq()
        .args(["pop", qdir.to_str().unwrap()])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let claimed = String::from_utf8(out).unwrap().trim().to_string();

    rfbmq()
        .args(["nack", qdir.to_str().unwrap(), &claimed])
        .assert()
        .success();

    // The message should be re-queued in pending/
    assert!(std::fs::read_dir(qdir.join("pending"))
        .unwrap()
        .any(|e| e.unwrap().file_name().to_string_lossy().ends_with(".md")));
}

// ── 13. reap ────────────────────────────────────────────────────────────

#[test]
fn test_cli_reap() {
    let tmp = TempDir::new().unwrap();
    let qdir = tmp.path().join("q");
    init_queue(&qdir);

    rfbmq()
        .args(["reap", qdir.to_str().unwrap()])
        .assert()
        .success()
        .stderr(predicate::str::contains("reaped"));
}

// ── 14. purge ───────────────────────────────────────────────────────────

#[test]
fn test_cli_purge() {
    let tmp = TempDir::new().unwrap();
    let qdir = tmp.path().join("q");
    init_queue(&qdir);

    rfbmq()
        .args(["purge", qdir.to_str().unwrap()])
        .assert()
        .success()
        .stderr(predicate::str::contains("purged"));
}

// ── 15. sync ────────────────────────────────────────────────────────────

#[test]
fn test_cli_sync() {
    let tmp = TempDir::new().unwrap();
    let qdir = tmp.path().join("q");
    init_queue(&qdir);

    rfbmq()
        .args(["sync", qdir.to_str().unwrap()])
        .assert()
        .success();
}

// ── 16. ready ───────────────────────────────────────────────────────────

#[test]
fn test_cli_ready() {
    let tmp = TempDir::new().unwrap();
    let qdir = tmp.path().join("q");
    init_queue(&qdir);

    // Push A (no deps — should be ready)
    let id_a = push_body(&qdir, "msg-a");

    // Push B that depends on A
    let out = rfbmq()
        .args([
            "push",
            qdir.to_str().unwrap(),
            "-d", &id_a,
        ])
        .write_stdin("msg-b")
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let id_b = String::from_utf8(out).unwrap().trim().to_string();

    // ready should output only A
    rfbmq()
        .args(["ready", qdir.to_str().unwrap()])
        .assert()
        .success()
        .stdout(predicate::str::contains(&id_a))
        .stdout(predicate::str::contains(&id_b).not());

    // Pop A and ack it to satisfy B's dependency
    let out = rfbmq()
        .args(["pop", qdir.to_str().unwrap()])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let claimed_a = String::from_utf8(out).unwrap().trim().to_string();

    rfbmq()
        .args(["ack", qdir.to_str().unwrap(), &claimed_a])
        .assert()
        .success();

    // Now ready should output B
    rfbmq()
        .args(["ready", qdir.to_str().unwrap()])
        .assert()
        .success()
        .stdout(predicate::str::contains(&id_b));
}

// ── 17. ready none ──────────────────────────────────────────────────────

#[test]
fn test_cli_ready_none() {
    let tmp = TempDir::new().unwrap();
    let qdir = tmp.path().join("q");
    init_queue(&qdir);

    rfbmq()
        .args(["ready", qdir.to_str().unwrap()])
        .assert()
        .code(1);
}

// ── 18. version ─────────────────────────────────────────────────────────

#[test]
fn test_cli_version() {
    rfbmq()
        .arg("version")
        .assert()
        .success()
        .stdout(predicate::str::contains("rfbmq 0.1.0"));
}

// ── 19. push queue full ─────────────────────────────────────────────────

#[test]
fn test_cli_push_queue_full() {
    let tmp = TempDir::new().unwrap();
    let qdir = tmp.path().join("q");

    init_queue_with_args(&qdir, &["--max-pending", "2"]);

    push_body(&qdir, "one");
    push_body(&qdir, "two");

    rfbmq()
        .args(["push", qdir.to_str().unwrap()])
        .write_stdin("three")
        .assert()
        .code(2)
        .stderr(predicate::str::contains("queue full"));
}

// ── 20. pop bad queue ───────────────────────────────────────────────────

#[test]
fn test_cli_pop_error_bad_queue() {
    rfbmq()
        .args(["pop", "/nonexistent"])
        .assert()
        .code(2);
}

// ── 21. push stdin (default, no file arg) ───────────────────────────────

#[test]
fn test_cli_push_stdin() {
    let tmp = TempDir::new().unwrap();
    let qdir = tmp.path().join("q");
    init_queue(&qdir);

    let id = push_body(&qdir, "stdin body");
    assert_eq!(id.len(), 32);

    let msg_path = first_md_in(&qdir.join("pending"));
    rfbmq()
        .args(["cat", &msg_path])
        .assert()
        .success()
        .stdout(predicate::str::contains("stdin body"));
}

// ── 22. push dash stdin ─────────────────────────────────────────────────

#[test]
fn test_cli_push_dash_stdin() {
    let tmp = TempDir::new().unwrap();
    let qdir = tmp.path().join("q");
    init_queue(&qdir);

    let out = rfbmq()
        .args(["push", qdir.to_str().unwrap(), "-"])
        .write_stdin("dash body")
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let id = String::from_utf8(out).unwrap().trim().to_string();
    assert_eq!(id.len(), 32);

    let msg_path = first_md_in(&qdir.join("pending"));
    rfbmq()
        .args(["cat", &msg_path])
        .assert()
        .success()
        .stdout(predicate::str::contains("dash body"));
}

// ── 23. empty purge ─────────────────────────────────────────────────────

#[test]
fn test_cli_empty_purge() {
    let tmp = TempDir::new().unwrap();
    let qdir = tmp.path().join("q");
    init_queue(&qdir);

    rfbmq()
        .args(["purge", qdir.to_str().unwrap()])
        .assert()
        .success()
        .stderr(predicate::str::contains("purged 0 message(s)"));
}

// ── 24. reply-to ────────────────────────────────────────────────────────

#[test]
fn test_cli_reply_to() {
    let tmp = TempDir::new().unwrap();
    let qdir = tmp.path().join("q");
    init_queue(&qdir);

    rfbmq()
        .args([
            "push",
            qdir.to_str().unwrap(),
            "-r", "/tmp/replies",
        ])
        .write_stdin("reply body")
        .assert()
        .success();

    let msg_path = first_md_in(&qdir.join("pending"));
    rfbmq()
        .args(["inspect", &msg_path])
        .assert()
        .success()
        .stdout(predicate::str::contains("Reply-To:       /tmp/replies"));
}

// ── 25. depends-on ──────────────────────────────────────────────────────

#[test]
fn test_cli_depends_on() {
    let tmp = TempDir::new().unwrap();
    let qdir = tmp.path().join("q");
    init_queue(&qdir);

    // Push a message so we have a valid ID to depend on
    let dep_id = push_body(&qdir, "dependency");

    rfbmq()
        .args([
            "push",
            qdir.to_str().unwrap(),
            "-d", &dep_id,
        ])
        .write_stdin("dependent msg")
        .assert()
        .success();

    // Find the second .md (the dependent message)
    let pending_dir = qdir.join("pending");
    let mut files: Vec<_> = std::fs::read_dir(&pending_dir)
        .unwrap()
        .filter_map(|e| {
            let e = e.ok()?;
            let name = e.file_name().to_string_lossy().into_owned();
            if name.ends_with(".md") {
                Some(e.path().to_string_lossy().into_owned())
            } else {
                None
            }
        })
        .collect();
    files.sort();
    // The second file (sorted by timestamp) should be the dependent one
    let dep_msg_path = &files[1];

    rfbmq()
        .args(["inspect", dep_msg_path])
        .assert()
        .success()
        .stdout(predicate::str::contains(&format!("Depends-On:     {}", dep_id)));
}
