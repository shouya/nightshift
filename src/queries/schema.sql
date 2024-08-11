CREATE TABLE inode(
    ino INTEGER PRIMARY KEY,
    size INTEGER NOT NULL,
    blocks INTEGER NOT NULL,
    atime_secs INTEGER NOT NULL,
    atime_nanos INTEGER NOT NULL,
    mtime_secs INTEGER NOT NULL,
    mtime_nanos INTEGER NOT NULL,
    ctime_secs INTEGER NOT NULL,
    ctime_nanos INTEGER NOT NULL,
    crtime_secs INTEGER NOT NULL,
    crtime_nanos INTEGER NOT NULL,
    kind INTEGER NOT NULL,
    perm INTEGER NOT NULL,
    nlink INTEGER NOT NULL,
    uid INTEGER NOT NULL,
    gid INTEGER NOT NULL,
    rdev INTEGER NOT NULL,
    blksize INTEGER NOT NULL,
    flags INTEGER NOT NULL
);

CREATE TABLE dir_entry (
    parent_ino INTEGER NOT NULL,
    name BLOB NOT NULL,
    ino INTEGER NOT NULL
);

CREATE INDEX entry_parent_ino_name_idx ON dir_entry (parent_ino, name);

CREATE TABLE block (
    ino INTEGER NOT NULL,
    offset INTEGER NOT NULL,
    end_offset INTEGER NOT NULL,
    size INTEGER NOT NULL,
    data BLOB NOT NULL
);

CREATE INDEX block_ino_offsets_idx ON block (ino, offset, end_offset);
