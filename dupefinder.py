#!/usr/bin/env python3

import sys
import os
import os.path
import json
import hashlib
import argparse
import logging
from logging import debug, info, warning

if sys.version_info < (3,4):
    raise SystemExit("Python < 3.4 not supported")

class DupeFileMap():

    def __init__(self, dir, *args, **kwargs):

        self._dirs = []
        self._dirs.append(dir)

        self._file_list = None
        self._old_hash_map = None
        opts = self._opts = {}
        opts["default-algorithm"] = "MD5"
        for k, v in kwargs.items():
            self._opts[k] = v
            if k == "old-map":
                self.import_old_file(v)

    def import_old(self, old_hash_struct):
        old_map = {}
        # TODO validate format ... expected_keys = ("Path", "Size", algorithm)

        # Import list of file records as map (path => file_info)
        if isinstance(old_hash_struct, list):
            for file_info in old_hash_struct:
                path = file_info["Path"]
                old_map[path] = file_info
        else:
            for path, file_info in old_hash_struct.items():
                old_map[path] = file_info

        # Store and return imported map
        self._old_hash_map = old_map
        return list(old_map.keys())

    def scan(self):
        # Initiate scan of (user-defined) search path
        file_list = self._file_list = []
        file_map = {}

        # Scanned directories
        scanned_dirs = set()
        orig_dev = None
        # Scan user-defined directory (multiple dirs possible)
        for dir in self._dirs:
            if orig_dev is None:
                orig_dev = os.stat(dir).st_dev

            # Scan directory recursively
            # Current directory, subdirectories, files
            for dir_path, dirs, files in os.walk(dir, followlinks=True):
                remaining_dirs = [] # subdirectories - already scanned dirs
                # Check subdirs, remove seen ones; shadow parent dir var
                for dir in dirs:
                    st = os.stat(os.path.join(dir_path, dir))
                    if st.st_dev != orig_dev:
                        # Dir on other filesystem
                        debug("found directory on other filesystem, skip: %s" % (dir))
                        continue
                    key = st.st_dev, st.st_ino
                    if key not in scanned_dirs:
                        scanned_dirs.add(key)
                        remaining_dirs.append(dir)
                # Update list of dirs yet to be searched
                dirs[:] = remaining_dirs
                # Add files in current dir to map
                for file in files:
                    path = os.path.join(dir_path, file)
                    st = os.stat(path)
                    if st.st_dev != orig_dev:
                        # File on other filesystem
                        debug("found file on other filesystem, skip: %s" % (file))
                        continue
                    file_data = {
                        "Path": path,
                        "FullPath": os.path.abspath(path),
                        "Name": file,
                        "Size": st.st_size,
                        "ModificationTime": int(st.st_mtime),
                        "Inum": st.st_ino,
                    }
                    file_list.append(file_data)
                    file_map[path] = file_data

        return file_map

    def hash(self):
        file_list = self._file_list
        if file_list is None:
            raise Exception("no file list, need to scan first")
        old_file_map = self._old_hash_map or {}

        for file_data in file_list[:]:
            path = file_data["Path"]
            name = os.path.basename(path)
            if not os.path.isfile(path):
                # File appears to have vanished
                warning("file vanished before it could be hashed: " + path)
                if self._opts.get("skip-vanished"):
                    file_list.remove(file_data) # iterating over copy, above
                    continue
                else:
                    raise Exception("file vanished before it could be hashed: " + path)
            old_file_data = old_file_map.get(path) # or undefined
            if not old_file_data:
                for data in old_file_map.values():
                    if data.get("FullPath") == file_data["FullPath"]:
                        old_file_data = data
                        break

            # Wanted hash algorithms dict
            algos = {
                "MD5": hashlib.md5(),
                "SHA1": hashlib.sha1(),
                "SHA224": hashlib.sha224(),
                "SHA256": hashlib.sha256(),
                "SHA384": hashlib.sha384(),
                "SHA512": hashlib.sha512(),
            }
            if "algorithms" in self._opts:
                # ctor > opts > algorithms
                user_algos = self._opts["algorithms"]
                for a in list(algos.keys()):
                    if a not in user_algos:
                        del algos[a]
            else:
                # Use default algorithm
                default_algorithm = self._opts["default-algorithm"]
                for a in list(algos.keys()):
                    if a != default_algorithm:
                        del algos[a]

            # Get previously calculated hashes from old map
            remaining_algos = algos.copy()
            for a in list(remaining_algos.keys()):
                if old_file_data and old_file_data.get(a):
                    # Got old hash, just check size hasn't changed
                    debug("file found in map: %s" % (name))
                    if old_file_data.get("Size") != file_data["Size"]:
                        # File has changed or different file, recalculate
                        debug("file found in map but wrong size, skip: %s" % (name))
                        continue
                    debug("file found in old map: %s" % (name))
                    file_data[a] = old_file_data[a]
                    del remaining_algos[a]

            # Calculate hashes
            with open(path, "rb") as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    for a, hashobj in remaining_algos.items():
                        hashobj.update(chunk)
                for a, hashobj in remaining_algos.items():
                    file_data[a] = hashobj.hexdigest()

        self._old_hash_map = self.map()
        return file_list.copy()

    def list(self):
        return self._file_list.copy()

    def map(self):
        return {x["Path"]:x for x in self.list()}

    def hash_map(self):
        hash_map = {}
        hash_algorithm = self._opts["default-algorithm"]
        if "algorithms" in self._opts:
            hash_algorithm = self._opts["algorithms"][0]
        for file_info in self.list():
            rel_path = file_info["Path"]
            hash = file_info.get(hash_algorithm) # hash == required key
            if hash is None:
                raise Exception("missing %s hash in file info record" % (hash_algorithm))
            # Exclude file if empty
            if not file_info["Size"]:
                continue
            # Collect all identical files, sort and remove hardlinks later
            hash_map[hash] = hash_map.get(hash, []) + [file_info]
        return hash_map

    def duplicates_map(self):
        file_map = self.map()
        duplicates_map = {}
        for hash, list in self.hash_map().items():
            # Get list of duplicate files, remove hardlinks
            seen_inums = []
            # TODO sort by ... mtime, name?
            sorted_list = list.copy()
            dup_list = []
            for file_info in sorted_list:
                current_inum = file_info["Inum"]
                if current_inum in seen_inums:
                    # Hardlink found, not a real duplicate
                    continue
                seen_inums.append(current_inum)
                dup_list.append(file_info)
            # Add file list to map if multiple duplicates found
            if len(dup_list) <= 1:
                continue
            duplicates_map[hash] = dup_list

        return duplicates_map

    def json(self):
        return json.dumps(self.list(), indent=4)

def parse_args():
    argparser = argparse.ArgumentParser(description="Find duplicate files.")
    argparser.add_argument("--import-map-file",
        help="map file to import, imported files won't be hashed (superficial scan)",
    )
    argparser.add_argument("--export-map-file",
        help="export map file that can be used to scan the same directory again",
    )
    argparser.add_argument("--link-duplicates", action="store_true",
        help="replace duplicates with hardlinks",
    )
    argparser.add_argument("--skip-vanished", action="store_true",
        help="skip files that vanished during the scan",
    )
    argparser.add_argument("--debug", action="store_true",
        help="enable debug logging",
    )
    argparser.add_argument("dir",
        help="search directory",
    )
    return argparser.parse_args()

def main():
    # Arguments
    args = parse_args()

    # Logging
    if args.debug:
        logging.basicConfig(level=logging.DEBUG)

    # Check search directory
    dir = args.dir
    if not os.path.isdir(dir):
        raise Exception("search path not found: " + dir)

    # Get started
    debug("Initiating scanner for selected directory: %s" % (dir))
    opts = {}
    if args.skip_vanished:
        opts["skip-vanished"] = True
    dupefilemap = DupeFileMap(dir, **opts)

    # Import old map
    file_map = {}
    if args.import_map_file:
        with open(args.import_map_file, "r") as file:
            old_hash_struct = json.load(file)
            imported = dupefilemap.import_old(old_hash_struct)
            debug("imported old map file (items: {})".format(len(imported)))

    # Start file scan
    debug("Starting file scan...")
    dupefilemap.scan()
    debug("Scan found %s file(s)" % (len(dupefilemap.list())))

    # Calculate hash sums
    dupefilemap.hash()

    # Import/export
    if args.export_map_file:
        with open(args.export_map_file, "w") as file:
            file.write(dupefilemap.json())

    # Go through list of duplicate groups
    duplicates_map = dupefilemap.duplicates_map()
    total_wasted_space = 0
    replaced_files = 0
    for hash, list in duplicates_map.items():
        group_wasted_space = 0
        print("[%s]" % (hash))
        for i, file_info in enumerate(list):
            cur_path = file_info["Path"]
            if i == 0:
                print("* %s" % (cur_path))
                src_path = cur_path
            else:
                group_wasted_space += file_info["Size"]
                print("- %s" % (cur_path))
                if args.link_duplicates:
                    print("    REPLACING #%s ..." % (file_info["Inum"]))
                    os.remove(cur_path)
                    os.link(src_path, cur_path)
                    replaced_files += 1
        total_wasted_space += group_wasted_space
        print()

    # Show summary
    total_file_count = len(dupefilemap.list())
    total_files_size = sum(x["Size"] for x in dupefilemap.list())
    print("Files:\t\t\t%s" % (total_file_count))
    print("Total size:\t\t%s B" % (total_files_size))
    print("Duplicate groups:\t%s" % (len(duplicates_map)))
    print("Total wasted space:\t%s B" % (total_wasted_space))
    print("Replaced files:\t\t%s" % (replaced_files))


if __name__ == '__main__':
    main()

