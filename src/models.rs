use std::{cmp, ffi::OsStr};

pub const BLOCK_SIZE: u64 = 16384;

pub struct ListDirEntry<'n> {
    pub offset: i64,
    pub ino: u64,
    pub name: &'n OsStr,
    pub kind: fuser::FileType,
}

pub struct Block {
    // Inode number.
    pub ino: u64,
    /// Block number.
    pub bno: u64,
    /// Block data. Always uncompressed.
    pub data: Vec<u8>,
}

impl Block {
    pub fn empty(ino: u64, bno: u64) -> Block {
        Block {
            ino,
            bno,
            data: Vec::new(),
        }
    }

    pub fn from_compressed(ino: u64, bno: u64, compressed_data: &[u8]) -> Block {
        let mut b = Block {
            ino,
            bno,
            data: vec![0u8; BLOCK_SIZE as usize],
        };
        let n = lz4_flex::decompress_into(compressed_data, &mut b.data).expect("lz4 decompress output too small");
        b.data.truncate(n);
        b
    }

    pub fn offset_to_bno(offset: u64) -> u64 {
        offset / BLOCK_SIZE
    }

    pub fn compress_into<'d>(&self, dest: &'d mut Vec<u8>) -> &'d [u8] {
        let max_size = lz4_flex::block::get_maximum_output_size(self.data.len());
        dest.resize(max_size, 0);
        let written = lz4_flex::compress_into(&self.data, dest).expect("lz4 compress output too small");
        &dest[..written]
    }

    fn start_offset(&self) -> u64 {
        self.bno * BLOCK_SIZE
    }

    fn end_offset(&self) -> u64 {
        (self.bno + 1) * BLOCK_SIZE
    }

    fn available(&self) -> u32 {
        u32::try_from(BLOCK_SIZE - self.data.len() as u64).expect("block size overflow")
    }

    pub fn consume(&mut self, data: &[u8]) -> u64 {
        let avail = self.available();
        let data_len = u32::try_from(data.len()).expect("data size overflow");
        let max_write = cmp::min(avail, data_len) as usize;
        self.data.extend_from_slice(&data[..max_write]);
        u64::try_from(max_write).expect("written overflow")
    }

    pub fn write_at(&mut self, inode_offset: u64, data: &[u8]) -> (u64, i64) {
        let start_len = self.data.len();
        let rel_offset = inode_offset - self.start_offset();
        self.data.resize(rel_offset as usize, 0);
        let written = self.consume(data);
        let diff = self.data.len() as i64 - start_len as i64;
        (written, diff)
    }

    pub fn copy_into(&self, dest: &mut Vec<u8>) -> usize {
        let remaining = dest.capacity() - dest.len();
        let max_write = cmp::min(remaining, self.data.len());
        dest.extend_from_slice(&self.data[..max_write]);
        max_write
    }

    pub fn truncate(&mut self, inode_offset: u64) {
        let rel_size = inode_offset - self.start_offset();
        self.data.truncate(rel_size as usize);
    }
}

impl std::fmt::Debug for Block {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Block")
            .field("ino", &self.ino)
            .field("bno", &self.bno)
            .field("start_offset", &self.start_offset())
            .field("end_offset", &self.end_offset())
            .field("data.len()", &self.data.len())
            .finish()
    }
}

#[test]
fn test_block() {
    let b = Block::empty(37, 1);
    assert_eq!(b.ino, 37);
    assert_eq!(b.start_offset(), BLOCK_SIZE);
    assert_eq!(b.end_offset(), BLOCK_SIZE + BLOCK_SIZE);
    assert_eq!(b.available(), BLOCK_SIZE as u32);
}

#[test]
fn test_block_consume() {
    let mut b = Block::empty(37, 0);
    assert_eq!(b.consume(&[0; 100]), 100);
    assert_eq!(b.consume(&[1; BLOCK_SIZE as usize]), BLOCK_SIZE - 100);
    assert!(b.data[..100].iter().all(|&b| b == 0));
    assert!(b.data[100..].iter().all(|&b| b == 1));
}

#[test]
fn test_block_write_at() {
    let mut b = Block::empty(0, 1);
    assert_eq!(b.write_at(BLOCK_SIZE, &[1; 5]), (5, 5));
    assert_eq!(b.data, vec![1; 5]);

    let mut b = Block::empty(0, 1);
    assert_eq!(b.write_at(BLOCK_SIZE + 5, &[1; 5]), (5, 10));
    assert_eq!(b.data, vec![0, 0, 0, 0, 0, 1, 1, 1, 1, 1]);
}

#[test]
fn test_block_copy_into() {
    let mut b = Block::empty(0, 1);
    b.data = vec![1; 10];

    let mut buf = Vec::with_capacity(5);
    assert_eq!(b.copy_into(&mut buf), 5);

    let mut buf = Vec::with_capacity(15);
    assert_eq!(b.copy_into(&mut buf), 10);
}

#[test]
fn test_block_offset_to_bno() {
    assert_eq!(Block::offset_to_bno(10000), 0);
    assert_eq!(Block::offset_to_bno(20000), 1);
}
