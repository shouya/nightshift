#![allow(clippy::too_many_arguments)]

mod database;
mod driver;
mod errors;
mod models;
mod queries;
mod time;
mod types;

use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thread,
    time::Duration,
};

use anyhow::Context;
use clap::Parser;

use crate::database::DatabaseOps;
use crate::driver::FuseDriver;
use simple_logger::SimpleLogger;

#[derive(Parser, Debug)]
struct Args {
    #[arg(short = 'p', long, default_value = "None")]
    password: Option<String>,
    #[arg(short = 'l', long, default_value = "info")]
    log_level: log::LevelFilter,
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    SimpleLogger::new()
        .with_level(args.log_level)
        .init()
        .context("unable to install logging")?;

    let db = DatabaseOps::open("foo.db", args.password.as_deref()).context("open db")?;
    let driver = FuseDriver::new(db);

    let mount = fuser::spawn_mount2(driver, "mnt-target", &[]).context("unable to create mount")?;

    let term = Arc::new(AtomicBool::new(false));
    signal_hook::flag::register(signal_hook::consts::SIGTERM, Arc::clone(&term))?;
    signal_hook::flag::register(signal_hook::consts::SIGINT, Arc::clone(&term))?;
    while !term.load(Ordering::Relaxed) {
        thread::sleep(Duration::from_millis(100));
    }
    drop(mount);
    Ok(())
}
