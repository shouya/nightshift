SELECT
    dir_entry.ino,
    dir_entry.name,
    inode.kind
FROM dir_entry
JOIN inode ON dir_entry.ino = inode.ino
WHERE
    dir_entry.parent_ino = ? -- folder being listed
    AND dir_entry.ino > ? -- offset by ino
ORDER BY dir_entry.ino ASC
