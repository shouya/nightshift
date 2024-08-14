use crate::{errors::Result, models::Block};
use rusqlite::params;

pub fn get_block_at(tx: &mut rusqlite::Transaction, ino: u64, offset: u64) -> Result<Block> {
    let mut stmt = tx.prepare_cached(
        "SELECT offset, end_offset, data FROM block WHERE ino = ? AND ? >= offset AND ? < end_offset",
    )?;
    let block = stmt.query_row(params![ino, offset, offset], |row| {
        let data = row.get_ref(2)?.as_blob()?;
        Ok(Block::from_compressed(ino, row.get(0)?, row.get(1)?, data))
    })?;
    Ok(block)
}

pub fn iter_blocks_from(
    tx: &mut rusqlite::Transaction,
    ino: u64,
    offset: u64,
    mut iter: impl FnMut(Block) -> bool,
) -> Result<()> {
    let mut stmt =
        tx.prepare_cached("SELECT offset, end_offset, data FROM block WHERE ino = ? AND offset >= ? ORDER BY offset")?;
    let mut rows = stmt.query(params![ino, offset])?;
    while let Some(row) = rows.next()? {
        let data = row.get_ref(2)?.as_blob()?;
        let block = Block::from_compressed(ino, row.get(0)?, row.get(1)?, data);
        let more = iter(block);
        if !more {
            break;
        }
    }
    Ok(())
}

pub fn update(tx: &mut rusqlite::Transaction, ino: u64, offset: u64, data: &[u8]) -> Result<(u64, i64)> {
    let mut block = get_block_at(tx, ino, offset)?;
    let (written, diff) = block.write_at(offset, data);
    let mut buf = Vec::new();
    let compressed_data = block.compress_into(&mut buf);

    let mut stmt = tx.prepare_cached("UPDATE block SET data = ? WHERE ino = ? AND offset = ?")?;
    stmt.execute(params![compressed_data, block.ino, block.offset])?;

    Ok((written, diff))
}

pub fn create(tx: &mut rusqlite::Transaction, ino: u64, offset: u64, data: &[u8]) -> Result<u64> {
    let mut block = Block::empty(ino, offset);
    let written = block.consume(data);
    let mut buf = Vec::new();
    let compressed_data = block.compress_into(&mut buf);

    let mut stmt = tx.prepare_cached("INSERT INTO block (ino, offset, end_offset, data) VALUES (?, ?, ?, ?)")?;
    stmt.execute(params![block.ino, block.offset, block.end_offset, compressed_data])?;

    Ok(written)
}

pub fn remove_blocks(tx: &mut rusqlite::Transaction, ino: u64) -> Result<()> {
    let mut stmt = tx.prepare_cached("DELETE FROM block WHERE ino = ?")?;
    stmt.execute(params![ino])?;
    Ok(())
}
