use std::fs::OpenOptions;
use std::io::Write;
use std::sync::{Arc, Mutex, mpsc};
use std::thread;

#[derive(Clone)]
pub struct PayloadLogger {
    log_stderr: bool,
    file_tx: Option<mpsc::Sender<String>>,
    stderr_lock: Arc<Mutex<()>>,
}

impl PayloadLogger {
    pub fn new(log_stderr: bool, record_path: Option<&str>) -> Self {
        let file_tx = record_path.and_then(|path| init_file_sender(path));
        Self {
            log_stderr,
            file_tx,
            stderr_lock: Arc::new(Mutex::new(())),
        }
    }

    pub fn is_active(&self) -> bool {
        self.log_stderr || self.file_tx.is_some()
    }

    pub fn log(&self, endpoint: &str, payload: &str) {
        if self.log_stderr {
            let _guard = self.stderr_lock.lock();
            eprintln!("[payload] {endpoint} {payload}");
        }
        if let Some(tx) = &self.file_tx {
            let ts = chrono_like_now();
            let line = format!("{ts}\t{endpoint}\t{}", payload.replace('\n', " "));
            let _ = tx.send(line);
        }
    }
}

fn init_file_sender(path: &str) -> Option<mpsc::Sender<String>> {
    let file = match OpenOptions::new().create(true).append(true).open(path) {
        Ok(f) => f,
        Err(err) => {
            eprintln!("failed to open payload log {path}: {err}");
            return None;
        }
    };
    let mut writer = std::io::BufWriter::new(file);
    let (tx, rx) = mpsc::channel::<String>();
    thread::spawn(move || {
        for line in rx {
            if writeln!(writer, "{line}").is_err() {
                break;
            }
            let _ = writer.flush();
        }
    });
    Some(tx)
}

fn chrono_like_now() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let d = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    format!("{}.{:09}", d.as_secs(), d.subsec_nanos())
}
