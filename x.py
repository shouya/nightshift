with open("mnt-target/foo", "wb") as f:
    f.write(b"\0" * 10000)
    f.seek(5000)
    f.write(b"\1" * 10000)
