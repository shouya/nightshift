import os
import os.path as osp
import hashlib
import sys


def iter_files(root: str):
    for dirpath, dirnames, filenames in os.walk(root):
        for name in filenames:
            yield osp.relpath(osp.join(dirpath, name), root)


def check_file(path: str):
    hasher = hashlib.md5()
    size = 0
    with open(path, "rb") as f:
        while True:
            data = f.read(4096)
            if not data:
                break
            hasher.update(data)
            size += len(data)
    return (hasher.hexdigest(), size)


def analyze(root: str):
    files = {}
    for name in iter_files(root):
        path = osp.join(root, name)
        files[name] = check_file(path)
    return files


source = analyze(sys.argv[1])
dest = analyze(sys.argv[2])

for name, data in source.items():
    try:
        dest_data = dest[name]
        if data != dest_data:
            print(f"Error: {name!r} {data!r} != {dest_data!r}")
        else:
            print(f"Ok: {name!r} {data!r} == {dest_data!r}")
    except KeyError:
        print(f"Not found in dest: {name}")

print(f"Source: {len(source)}")
print(f"Dest: {len(dest)}")

print(sum(p[1] for p in source.values()))
print(sum(p[1] for p in dest.values()))

# short_names = [(osp.basename(k), v) for k, v in source.items()]
# short_names.sort(key=lambda p: p[0])
# for x in short_names:
#     print(x)
