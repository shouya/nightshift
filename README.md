# nightshift

`nightshift` implements a POSIX filesystem within a SQLite database using [FUSE](https://en.wikipedia.org/wiki/Filesystem_in_Userspace).

- Benefit from SQLite's renowned durability, portability and [well-documented storage format](https://www.sqlite.org/draft/locrsf.html).
- Encryption is handled by SQLCipher, a battle tested library built on top of SQLite. SQLCipher uses AES-256.
- Compression is handled by lz4, providing fast compression and decompression speeds
- Hackable project: the filesystem is implemented with simple SQL queries and a few tables

## Example: backup files with `mount-exec`

The `mount-exec` command will mount the given database at the given path using
the given key and then run the command given in the `--cmd` arg using the optional
`--arg` arguments. Once the script exits, the filesystem is safely unmounted and
`nightshift` will exits with a status that indicates if the script failed or not.

```bash
nightshift mount-exec --db /tank/data/backup.db --mount "$(mktemp -d)" --key-file /opt/backup/key.txt --cmd /opt/backup/backup.bash
```

Write a callback script to backup precious files inside of the mounted filesystem.
Use any command you like inside the script:

```bash
# file: /opt/backup/backup.bash
#!/bin/bash

cp -r /tank/data/photos "$NIGHTSHIFT_MOUNT_PATH"

pgdump ... | gzip > "$NIGHTSHIFT_MOUNT_PATH/databases.sql.gz"

scp -r myuser@server.example.com:/data "$NIGHTSHIFT_MOUNT_PATH"
```

`nightshift` always sets the `NIGHTSHIFT_MOUNT_PATH` and `NIGHTSHIFT_DB_PATH` environment
variables inside the callback script.

## Why is it called nightshift?

Naming projects is hard. I was listening to the song [Nightshift](https://www.youtube.com/watch?v=FrkEDe6Ljqs)
from the Commodores when I started this project. It seemed like an acceptable name at the time.
