use std::{cmp, ffi::OsStr};

pub const BLOCK_SIZE: u32 = 4096 * 4;

pub struct ListDirEntry<'n> {
    pub offset: i64,
    pub ino: u64,
    pub name: &'n OsStr,
    pub kind: fuser::FileType,
}

pub struct Block {
    pub ino: u64,
    pub offset: u64,
    pub end_offset: u64,
    pub data: Vec<u8>, // UNCOMPRESSED ALWAYS
}

impl Block {
    pub fn empty(ino: u64, offset: u64) -> Block {
        Self::empty_with_size(ino, offset, BLOCK_SIZE)
    }

    pub fn empty_with_size(ino: u64, offset: u64, size: u32) -> Block {
        Block {
            ino,
            offset,
            end_offset: offset + u64::from(size),
            data: Vec::new(),
        }
    }

    pub fn from_compressed(ino: u64, offset: u64, end_offset: u64, compressed_data: &[u8]) -> Block {
        let mut b = Block {
            ino,
            offset,
            end_offset,
            data: vec![0u8; BLOCK_SIZE as usize],
        };
        let n = lz4_flex::decompress_into(compressed_data, &mut b.data).expect("lz4 decompress output too small");
        b.data.truncate(n);
        b
    }

    pub fn compress_into<'d>(&self, dest: &'d mut Vec<u8>) -> &'d [u8] {
        let max_size = lz4_flex::block::get_maximum_output_size(self.data.len());
        dest.resize(max_size, 0);
        let written = lz4_flex::compress_into(&self.data, dest).expect("lz4 compress output too small");
        &dest[..written]
    }

    fn available(&self) -> u32 {
        let block_size = self.end_offset - self.offset;
        u32::try_from(block_size - self.data.len() as u64).expect("block size overflow")
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
        let rel_offset = inode_offset - self.offset;
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
}

impl std::fmt::Debug for Block {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Block")
            .field("ino", &self.ino)
            .field("offset", &self.offset)
            .field("end_offset", &self.end_offset)
            .field("data.len()", &self.data.len())
            .finish()
    }
}

#[test]
fn test_block() {
    let b = Block::empty_with_size(37, 4096, 1024);
    assert_eq!(b.ino, 37);
    assert_eq!(b.offset, 4096);
    assert_eq!(b.end_offset, 4096 + 1024);
    assert_eq!(b.available(), 1024);
}

#[test]
fn test_block_consume() {
    let mut b = Block::empty_with_size(37, 0, 10);
    assert_eq!(b.consume(&[0; 5]), 5);
    assert_eq!(b.consume(&[1; 10]), 5);
    assert_eq!(b.data, vec![0, 0, 0, 0, 0, 1, 1, 1, 1, 1]);
}

#[test]
fn test_block_write_at() {
    let mut b = Block::empty_with_size(0, 100, 10);
    assert_eq!(b.write_at(100, &[1; 5]), (5, 5));
    assert_eq!(b.data, vec![1; 5]);

    let mut b = Block::empty_with_size(0, 100, 10);
    assert_eq!(b.write_at(105, &[1; 5]), (5, 10));
    assert_eq!(b.data, vec![0, 0, 0, 0, 0, 1, 1, 1, 1, 1]);
}

#[test]
fn test_block_copy_into() {
    let mut b = Block::empty_with_size(0, 100, 10);
    b.data = vec![1; 10];

    let mut buf = Vec::with_capacity(5);
    assert_eq!(b.copy_into(&mut buf), 5);

    let mut buf = Vec::with_capacity(15);
    assert_eq!(b.copy_into(&mut buf), 10);
}
