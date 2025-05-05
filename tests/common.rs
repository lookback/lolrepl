use std::fs;
use std::io::Write;
use std::path::PathBuf;
use std::process::Command;
use std::process::Stdio;
use std::str;

// Fixed port for the test PostgreSQL server
const PG_TEST_PORT: u16 = 23998;

pub struct TempDb {
    pub port: u16,
    pub data_dir: PathBuf,
}

impl TempDb {
    pub fn execute(&self, s: &str) {
        let mut child = Command::new("psql")
            .args(&[
                "-p",
                &self.port.to_string(),
                "-U",
                "postgres",
                "-d",
                "testing",
            ])
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("Failed to spawn psql process");

        if let Some(mut stdin) = child.stdin.take() {
            stdin
                .write_all(s.as_bytes())
                .expect("Failed to write to stdin");
            // Dropping stdin will close it
        }

        let _ = child.wait().expect("Failed to wait on psql");
    }
}

impl Drop for TempDb {
    fn drop(&mut self) {
        // Stop PostgreSQL server
        let output = Command::new("pg_ctl")
            .args(&["-D", self.data_dir.to_str().unwrap(), "stop", "-m", "fast"])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .output();

        match output {
            Ok(_) => {}
            Err(e) => println!("Failed to stop PostgreSQL: {}", e),
        }
    }
}

// Check if port is in use and kill any process using it
fn ensure_port_available() {
    // First, check if we can find a data directory in the .postgres folder
    let data_dir = PathBuf::from(".postgres");
    if data_dir.exists() {
        // Try to stop PostgreSQL gracefully first with pg_ctl
        let pg_ctl_output = Command::new("pg_ctl")
            .args(&["-D", data_dir.to_str().unwrap(), "stop", "-m", "fast"])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .output();

        match pg_ctl_output {
            Ok(output) => {
                if !output.status.success() {
                    // This is ok.
                }
            }
            Err(e) => println!("Failed to stop PostgreSQL with pg_ctl: {}", e),
        }
    }
}

pub fn init_tmp_db() -> TempDb {
    // Check for PostgreSQL installation
    let pg_version = Command::new("pg_config").arg("--version").output();

    match pg_version {
        Ok(_) => {}
        Err(e) => {
            println!("WARNING: Could not verify PostgreSQL installation: {}", e);
        }
    }

    // Use fixed port and ensure it's available
    let port = PG_TEST_PORT;
    ensure_port_available();

    // Use fixed .postgres directory in current directory
    let data_dir = PathBuf::from(".postgres");

    // Remove old directory if it exists
    if data_dir.exists() {
        match fs::remove_dir_all(&data_dir) {
            Ok(_) => {}
            Err(e) => println!("Failed to remove existing directory: {}", e),
        }
    }

    // Convert PathBuf to string for commands
    let data_dir_str = data_dir.to_str().unwrap();

    // Initialize database with postgres user
    let initdb_args = &[
        "--locale-provider=icu",
        "--icu-locale=en",
        "--username=postgres", // Specify postgres as the superuser
        "-D",
        data_dir_str,
    ];

    let output = Command::new("initdb")
        .args(initdb_args)
        .output()
        .expect("Failed to initialize database");

    if !output.status.success() {
        panic!("Failed to initialize database: {:?}", output);
    }

    // Create log file directory
    let log_dir = data_dir.join("logs");
    fs::create_dir_all(&log_dir).expect("Failed to create log directory");
    let log_file = log_dir.join("postgres.log");
    let log_file_str = log_file.to_str().unwrap();

    // Update postgresql.conf to enable replication
    let postgresql_conf_path = data_dir.join("postgresql.conf");
    let conf_content =
        fs::read_to_string(&postgresql_conf_path).expect("Failed to read postgresql.conf");

    // Add replication settings to the end of the file
    let replication_settings = format!(
        "
# Replication settings
wal_level = logical
max_wal_senders = 10
max_replication_slots = 10

# Timezone settings
timezone = 'Asia/Kolkata'
log_timezone = 'Asia/Kolkata'
"
    );

    let new_conf = conf_content + &replication_settings;
    fs::write(&postgresql_conf_path, new_conf).expect("Failed to update postgresql.conf");

    // Start PostgreSQL server
    let pg_ctl_args = &[
        "-D",
        data_dir_str,
        "-l",
        log_file_str,
        "-o",
        &format!("-F -p {} -c log_min_messages=warning", port),
        "start",
    ];

    let pg_ctl_output = Command::new("pg_ctl")
        .args(pg_ctl_args)
        .output()
        .expect("Failed to start PostgreSQL server");

    if !pg_ctl_output.status.success() {
        // If we failed to start, check the log file for more details
        if log_file.exists() {
            match fs::read_to_string(&log_file) {
                Ok(log_content) => eprintln!("PostgreSQL log file contents:\n{}", log_content),
                Err(e) => eprintln!("Failed to read log file: {}", e),
            }
        }

        panic!("Failed to start PostgreSQL server: {:?}", pg_ctl_output);
    }

    // Check if PostgreSQL is actually running
    let pg_isready = Command::new("pg_isready")
        .args(&["-p", &port.to_string()])
        .output();

    match pg_isready {
        Ok(_) => {}
        Err(e) => eprintln!("Failed to check if PostgreSQL is ready: {}", e),
    }

    // Create testing database
    let createdb_output = Command::new("createdb")
        .args(&["-p", &port.to_string(), "-U", "postgres", "testing"])
        .output()
        .expect("Failed to create database");

    // It's fine if the database already exists
    if !createdb_output.status.success() {
        eprintln!("Note: testing database might already exist");
    }

    TempDb { port, data_dir }
}
