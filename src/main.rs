use std::{path::Path, io::Read};
use notify::{RecursiveMode, Watcher};
use tokio;
use std::io::BufReader;
use serde_json::{Value};

#[macro_use] extern crate log;
extern crate simplelog;

use simplelog::*;
use std::fs::File;

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
    info!("FS Changes detected on {}, backup scheduled in {} seconds.",job.path,job.throttle);
    tokio::time::sleep(std::time::Duration::from_secs(job.throttle)).await;
    info!("{} Backup on {} initiating.",job.name,job.path);
    let job = job.clone();
    let config = config.clone();
    let mut cmd = std::process::Command::new(config.restic_path)
        .env("PATH",config.env_path)
        .env("RESTIC_PASSWORD_COMMAND",config.password_command)
        .arg("-r")
        .arg(config.repo)
        .arg("--json")
        .arg("-q")
        .arg("--exclude-file")
        .arg(config.exclude_file)
        .arg("backup")
        .arg(job.path)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn().expect("Failed to spawn restic process.");

    let mut reader = BufReader::new(cmd.stdout.take().unwrap());
    let mut err_reader = BufReader::new(cmd.stderr.take().expect("No err captured"));
    
    let mut result = String::new();
    if reader.read_to_string(&mut result).is_err() {error!("Unable to parse response from restic.");}
    cmd.wait();
    match serde_json::from_str::<Value>(&result) {
        Ok(v) => {
            info!("Backup Complete. - {} new, {} changed, finished in {} seconds.", v["files_new"], v["files_changed"], v["total_duration"]);
        },
        Err(_) => {
            error!("Unable to parse restic response json: Raw resp: {}",result);
        }
    };
    
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
    info!("Started FSEvent monitoring on {} named {} - interval={}",&job.path,&job.name,&job.throttle);
    loop {
        rx.recv().await.unwrap();
        tokio::select! {
            _ = backup(&job,config) => {},
            _ = async {
                loop {
                    rx.recv().await;
                }
            } => {}
        }
    }
}

async fn unlock_repository(config:&BackupConfig) {
    info!("Unlocking Repository");
    let config = config.clone();
    info!("Attempting to remove stale lock");
    std::process::Command::new(config.restic_path)
        .env("PATH",config.env_path)
        .env("RESTIC_PASSWORD_COMMAND",config.password_command)
        .arg("-r")
        .arg(config.repo)
        .arg("unlock")
        .stdout(std::process::Stdio::inherit())
        .stderr(std::process::Stdio::inherit())
        .spawn().expect("Failed to spawn restic process.").wait().expect("Failed to remove stale lock.");
    info!("Lock removed success.")
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

    // Configure Logging
    let term_logger = TermLogger::new(LevelFilter::Info, Config::default(), TerminalMode::Mixed, ColorChoice::Auto);
    let write_logger = WriteLogger::new(LevelFilter::Info, Config::default(), File::create(&config.logfile).expect("Unable to create logfile."));
    CombinedLogger::init(vec![term_logger,write_logger]).unwrap();

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