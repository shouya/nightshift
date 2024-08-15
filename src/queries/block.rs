use crate::{errors::Result, models::Block};
use rusqlite::params;

pub fn get_block(tx: &mut rusqlite::Transaction, ino: u64, bno: u64) -> Result<Block> {
    let mut stmt = tx.prepare_cached("SELECT bno, data FROM block WHERE ino = ? AND bno >= ? ORDER BY bno")?;
    let block = stmt.query_row(params![ino, bno], |row| {
        let data = row.get_ref(1)?.as_blob()?;
        let block = Block::from_compressed(ino, row.get(0)?, data);
        Ok(block)
    })?;
    Ok(block)
}

pub fn iter_blocks_from(
    tx: &mut rusqlite::Transaction,
    ino: u64,
    offset: u64,
    mut iter: impl FnMut(Block) -> Result<bool>,
) -> Result<()> {
    let bno = Block::offset_to_bno(offset);
    let mut stmt = tx.prepare_cached("SELECT bno, data FROM block WHERE ino = ? AND bno >= ? ORDER BY bno")?;
    let mut rows = stmt.query(params![ino, bno])?;
    while let Some(row) = rows.next()? {
        let data = row.get_ref(1)?.as_blob()?;
        let block = Block::from_compressed(ino, row.get(0)?, data);
        let more = iter(block)?;
        if !more {
            break;
        }
    }
    Ok(())
}

pub fn update(tx: &mut rusqlite::Transaction, block: &Block) -> Result<()> {
    let mut buf = Vec::new();
    let compressed_data = block.compress_into(&mut buf);

    let mut stmt = tx.prepare_cached("UPDATE block SET data = ? WHERE ino = ? AND bno = ?")?;
    stmt.execute(params![compressed_data, block.ino, block.bno])?;

    Ok(())
}

pub fn create(tx: &mut rusqlite::Transaction, ino: u64, offset: u64, data: &[u8]) -> Result<u64> {
    let bno = Block::offset_to_bno(offset);
    let mut block = Block::empty(ino, bno);
    let written = block.consume(data);
    let mut buf = Vec::new();
    let compressed_data = block.compress_into(&mut buf);

    let mut stmt = tx.prepare_cached("INSERT INTO block (ino, bno, data) VALUES (?, ?, ?)")?;
    stmt.execute(params![block.ino, block.bno, compressed_data])?;

    Ok(written)
}

pub fn remove_blocks_from(tx: &mut rusqlite::Transaction, ino: u64, bno: u64) -> Result<()> {
    let mut stmt = tx.prepare_cached("DELETE FROM block WHERE ino = ? AND bno >= ?")?;
    stmt.execute(params![ino, bno])?;
    Ok(())
}
