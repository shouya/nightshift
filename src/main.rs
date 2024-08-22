#![allow(clippy::too_many_arguments)]

mod database;
mod driver;
mod errors;
mod queries;
mod time;
mod types;

use std::{
    fs,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thread,
    time::Duration,
};

use anyhow::{bail, Context};
use clap::{Parser, Subcommand};

use crate::database::DatabaseOps;
use crate::driver::FuseDriver;
use simple_logger::SimpleLogger;

#[derive(Parser, Debug)]
struct Cli {
    #[arg(short = 'l', long, default_value = "info")]
    log_level: log::LevelFilter,

    #[arg(long = "db", help = "Database file path")]
    database_path: PathBuf,

    #[arg(long = "dest", help = "Path where filesystem will be mounted")]
    mount_path: PathBuf,

    #[clap(flatten)]
    key_group: KeyGroup,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, clap::Args)]
#[group(required = true, multiple = false)]
struct KeyGroup {
    #[arg(long, help = "Decryption key")]
    key: Option<String>,

    #[arg(long, help = "Path to file containing decryption key")]
    key_file: Option<PathBuf>,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Mount the database on a directory.
    Mount,
    /// Optimize the database file and reduce disk space usage.
    Optimize,
}

fn main() -> anyhow::Result<()> {
    let args = Cli::parse();

    SimpleLogger::new()
        .with_level(args.log_level)
        .init()
        .context("unable to install logging")?;

    let key: String = if let Some(key) = args.key_group.key {
        key
    } else if let Some(key_file) = args.key_group.key_file {
        read_key(&key_file)?
    } else {
        bail!("One of --key or --key-file is required");
    };

    if key.is_empty() {
        bail!("Key cannot be empty");
    }

    match args.command {
        Commands::Mount => {
            let db = DatabaseOps::open(&args.database_path, key).context("open db")?;
            let driver = FuseDriver::new(db);

            let mount = fuser::spawn_mount2(driver, &args.mount_path, &[]).context("unable to create mount")?;

            let term = Arc::new(AtomicBool::new(false));
            signal_hook::flag::register(signal_hook::consts::SIGTERM, Arc::clone(&term))?;
            signal_hook::flag::register(signal_hook::consts::SIGINT, Arc::clone(&term))?;
            while !term.load(Ordering::Relaxed) {
                thread::sleep(Duration::from_millis(100));
            }
            // Umount & cleanup
            mount.join();
        }
        Commands::Optimize => {
            let mut db = DatabaseOps::open(&args.database_path, key).context("open db")?;
            println!("Running VACUUM on database, this may take a few seconds...");
            db.vacuum()?;
            println!("Done!");
        }
    };

    Ok(())
}

fn read_key(key_file: &Path) -> anyhow::Result<String> {
    let raw_key = fs::read_to_string(key_file)?;
    Ok(raw_key.trim_end().to_owned())
}
