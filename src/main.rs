use std::{path::Path, io::Write};
use notify::{RecursiveMode, Watcher};
use tokio;
use std::io::BufReader;
use chrono::Utc;

#[derive(Clone)]
struct BackupConfig {
    repo: String,
    exclude_file: String,
    password_command: String,
    logfile: String,
    env_path: String,
    restic_path: String
}
#[derive(Clone)]
struct BackupJobConfig {
    name: String,
    path: String,
    throttle: u64
}

async fn backup(job:&BackupJobConfig,config:&BackupConfig) -> Result<(),()>{
    log(format!("FS Changes detected on {}, backup scheduled in {} seconds.",job.path,job.throttle),&config.logfile);
    tokio::time::sleep(std::time::Duration::from_secs(job.throttle)).await;
    log(format!("{} Backup on {} initiating.",job.name,job.path),&config.logfile);
    let job = job.clone();
    let config = config.clone();
    let mut cmd = std::process::Command::new(config.restic_path)
        .env("PATH",config.env_path)
        .env("RESTIC_PASSWORD_COMMAND",config.password_command)
        .arg("-r")
        .arg(config.repo)
        .arg("--exclude-file")
        .arg(config.exclude_file)
        .arg("backup")
        .arg(job.path)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn().expect("Failed to spawn restic process.");

    let mut reader = BufReader::new(cmd.stdout.take().unwrap());
    let mut err_reader = BufReader::new(cmd.stderr.take().expect("No err captured"));

    let mut stdout = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .write(true)
        .open(config.logfile)
        .expect("open file error");
    let mut stderr = stdout.try_clone().expect("Error while trying to clone file instance.");
    std::io::copy(&mut reader, &mut stdout).expect("Unable to attach stdout");
    std::io::copy(&mut err_reader, &mut stderr).expect("Unable to attach stderr");
    Ok(())
}

async fn start_watching(job:BackupJobConfig,config:&BackupConfig) {
    let (tx,mut rx) = tokio::sync::mpsc::unbounded_channel::<String>();
    let mut watcher = notify::recommended_watcher(move |res| {
        match res {
           Ok(_) => {tx.send("hit".to_owned()).expect("Failed to send message over channel.");},
           Err(_) => {}
        }
    }).unwrap();
    watcher.watch(Path::new(&job.path), RecursiveMode::Recursive).expect(&format!("Failed to start watching on path {}",&job.path));
    log(format!("Started FSEvent monitoring on {} named {} - interval={}",&job.path,&job.name,&job.throttle),&config.logfile);
    loop {
        rx.recv().await.unwrap();
        tokio::select! {
            _ = backup(&job,config) => {log(format!("Backup finished."),&config.logfile)},
            _ = async {
                loop {
                    rx.recv().await;
                }
            } => {}
        }
    }
}

fn log<S>(msg:S,logfile_path:&String)
where S: Into<String>
{
    let msg = format!("{} {} \n",Utc::now().format("%Y-%m-%d %H:%M:%S ").to_string(),msg.into());
    let mut logfile = std::fs::OpenOptions::new()
        .append(true)
        .open(logfile_path)
        .expect("Unable to open logfile.");
    logfile.write(msg.as_bytes()).expect("Unable to write to log");
}

async fn unlock_repository(config:&BackupConfig) {
    let config = config.clone();
    log("Attempting to remove stale lock", &config.logfile);
    std::process::Command::new(config.restic_path)
        .env("PATH",config.env_path)
        .env("RESTIC_PASSWORD_COMMAND",config.password_command)
        .arg("-r")
        .arg(config.repo)
        .arg("unlock")
        .stdout(std::process::Stdio::inherit())
        .stderr(std::process::Stdio::inherit())
        .spawn().expect("Failed to spawn restic process.").wait().expect("Failed to remove stale lock.");
    
}

#[tokio::main]
async fn main() {
    let config_path = std::env::args().nth(1).unwrap_or("config.yml".to_owned());
    let f= std::fs::read_to_string(config_path).expect("Failed to read config file.");
    let y  = yaml_rust::YamlLoader::load_from_str(&f).expect("Failed to parse yaml");
    
    let config = BackupConfig {
        repo: y[0]["repo"].as_str().expect("Failed to parse repo from config").to_owned(),
        exclude_file: y[0]["exclude-file"].as_str().unwrap().to_owned(),
        password_command: y[0]["password-command"].as_str().unwrap().to_owned(),
        logfile: y[0]["logfile"].as_str().unwrap().to_owned(),
        env_path: y[0]["env-path"].as_str().unwrap().to_owned(),
        restic_path: y[0]["restic-path"].as_str().unwrap().to_owned()
    };

    unlock_repository(&config).await;

    let mut dirs = vec![];

    for dir in y[0]["dirs"].as_vec().unwrap() {
            dirs.push(
                start_watching(BackupJobConfig {
                    name: dir["name"].as_str().unwrap().to_owned(),
                    path: dir["path"].as_str().unwrap().to_owned(),
                    throttle: dir["throttle"].as_i64().unwrap() as u64
                },&config)
            )
    }

    futures::future::join_all(dirs).await;
}