CREATE TABLE inode(
    ino INT PRIMARY KEY,
    size INT NOT NULL,
    blocks INT NOT NULL,
    atime_secs INT NOT NULL,
    atime_nanos INT NOT NULL,
    mtime_secs INT NOT NULL,
    mtime_nanos INT NOT NULL,
    ctime_secs INT NOT NULL,
    ctime_nanos INT NOT NULL,
    crtime_secs INT NOT NULL,
    crtime_nanos INT NOT NULL,
    kind INT NOT NULL,
    perm INT NOT NULL,
    nlink INT NOT NULL,
    uid INT NOT NULL,
    gid INT NOT NULL,
    rdev INT NOT NULL,
    blksize INT NOT NULL,
    flags INT NOT NULL
);

CREATE TABLE dir_entry (
    parent_ino INT NOT NULL,
    name BLOB NOT NULL,
    ino INT NOT NULL
);

CREATE INDEX entry_parent_ino_name_idx ON entry (parent_ino, name);
