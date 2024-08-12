#![allow(clippy::too_many_arguments)]

mod database;
mod driver;
mod errors;
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

use crate::database::DatabaseOps;
use crate::driver::FuseDriver;
use simple_logger::SimpleLogger;

fn main() -> anyhow::Result<()> {
    SimpleLogger::new()
        .with_level(log::LevelFilter::Trace)
        .init()
        .context("unable to install logging")?;

    let driver = FuseDriver {
        db: DatabaseOps::open("foo.db")?,
    };

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
