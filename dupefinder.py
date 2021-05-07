#!/usr/bin/env python3

import sys
from sys import exit
import os
import os.path
import json
import hashlib
import argparse
import logging
import contextlib
from logging import debug, info, warning

if sys.version_info < (3,4):
    raise SystemExit("Python < 3.4 not supported")


@contextlib.contextmanager
def file_open(filename=None, mode=''):
    if filename and filename != '-':
        fh = open(filename, mode)
    else:
        fh = sys.stdout

    try:
        yield fh
    finally:
        if fh is not sys.stdout:
            fh.close()


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

    def user_algorithms(self, default=False):
        user_algorithms = []
        # List of user-defined algorithms (may be empty)
        # ctor > opts > algorithms
        if self._opts.get("algorithms"):
            for user_algo in self._opts.get("algorithms", []):
                user_algorithms.append(user_algo)
        # Return default algorithm only if requested (instead of an empty list)
        if not user_algorithms and default:
            user_algorithms.append(self._opts["default-algorithm"])
        return user_algorithms

    def default_algorithm(self):
        return self.user_algorithms(default=True)[0]

    def algorithms(self, map=False, all=False):
        python_list = hashlib.algorithms_available
        user_list = [x.lower() for x in self.user_algorithms(default=True)]
        requested_list = python_list if all else user_list
        # Map of standard hash algorithms, with hash generator object
        algos = {}
        # This may be preferred but it mixes up the order
        # if "sha1" in requested_list: algos["SHA1"] = hashlib.sha1()
        # if "sha224" in requested_list: algos["SHA224"] = hashlib.sha224()
        # if "sha256" in requested_list: algos["SHA256"] = hashlib.sha256()
        # if "sha384" in requested_list: algos["SHA384"] = hashlib.sha384()
        # if "sha512" in requested_list: algos["SHA512"] = hashlib.sha512()
        # MD5 might be missing because someone wanted to be "FIPS compliant"
        if "md5" in requested_list:
            if "md5" in python_list:
                algos["MD5"] = hashlib.md5()
            elif "md5" in user_list:
                raise Exception("MD5 requested but not available")
        # Add more algorithms if available or requested
        for algo in requested_list:
            if algo.upper() not in algos:
                try:
                    hashobj = hashlib.new(algo)
                    algos[algo.upper()] = hashobj
                except ValueError as e:
                    raise SystemExit("unknown hash algorithm: %s" % (algo))

        if map:
            return algos
        else:
            return algos.keys()

    def hash(self):
        file_list = self._file_list
        if file_list is None:
            raise Exception("no file list, need to scan first")
        old_file_map = self._old_hash_map or {}
        file_inum_map = self._file_inum_map = {}
        self._files_with_imported_hash = []

        for file_data in file_list[:]:
            path = file_data["Path"]
            name = os.path.basename(path)
            inum = file_data["Inum"]
            # Check if file still exists (time may have passed since scan)
            if not os.path.isfile(path):
                # File appears to have vanished
                if self._opts.get("ignore-vanished"):
                    debug("file vanished before it could be hashed: " + path)
                    file_list.remove(file_data) # iterating over copy, above
                    continue
                elif self._opts.get("fatal-vanished"):
                    raise Exception("file vanished before it could be hashed: " + path)
                warning("file vanished before it could be hashed: " + path)

            # Old file info and hash from imported map
            old_file_data = old_file_map.get(path) # or undefined
            if not old_file_data:
                for data in old_file_map.values(): # TODO optional... --use-full-path
                    if data.get("FullPath") == file_data["FullPath"]:
                        old_file_data = data
                        break
            got_old_file_date = old_file_data is not None

            # Wanted hash algorithms dict
            algos = self.algorithms(map=True)

            # Get previously calculated hashes from old map
            debug(path)
            remaining_algos = algos.copy()
            if file_inum_map.get(inum):
                # Hardlink already hashed
                debug("file #%d already hashed: %s" % (inum, name))
                old_file_data = file_inum_map.get(inum)
            for a in list(remaining_algos.keys()):
                if old_file_data and old_file_data.get(a):
                    # Got old hash, just check size hasn't changed
                    if old_file_data.get("Size") != file_data["Size"]:
                        # File has changed or different file, recalculate
                        debug("file found in map but wrong size, skip: %s" % (name))
                        continue
                    debug("%s file hash already map: %s" % (a, name))
                    file_data[a] = old_file_data[a]
                    del remaining_algos[a]

            # Calculate hashes
            with open(path, "rb") as f:
                if remaining_algos:
                    debug("calculating hash sum(s) (%s) for: %s" % (", ".join(remaining_algos.keys()), name))
                    file_inum_map[inum] = file_data
                    for chunk in iter(lambda: f.read(4096), b""):
                        for a, hashobj in remaining_algos.items():
                            hashobj.update(chunk)
                else:
                    debug("not recalculating hash: %s" % (name))
                    if got_old_file_date:
                        self._files_with_imported_hash.append(path)

                for a, hashobj in remaining_algos.items():
                    file_data[a] = hashobj.hexdigest()

        self._old_hash_map = self.map()
        return file_list.copy()

    def list(self):
        return self._file_list.copy()

    def map(self):
        return {x["Path"]:x for x in self.list()}

    def imported_hashed_files(self):
        return self._files_with_imported_hash.copy()

    def hash_map(self):
        hash_map = {}
        hash_algorithm = self.default_algorithm()
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
        help="map file to import, imported files won't be hashed again (superficial scan)",
    )
    argparser.add_argument("--export-map-file",
        help="export map file that can be used to scan the same directory again, saving time",
    )
    argparser.add_argument("--export-hashsums-file",
        help="export file hash table (like md5sums)",
    )
    argparser.add_argument("--link-duplicates", action="store_true",
        help="remove and replace duplicates with hardlinks (DANGER!)",
    )
    argparser.add_argument("--ignore-vanished", action="store_true",
        help="don't warn about files that vanished during the scan",
    )
    argparser.add_argument("--fatal-vanished", action="store_true",
        help="abort if files vanished during the scan",
    )
    argparser.add_argument("--hide-summary", action="store_true",
        help="hide summary (footer)",
    )
    # TODO --use-full-path
    argparser.add_argument("--quiet", action="store_true",
        help="hide standard output, don't list duplicates",
    )
    # TODO MD5 might not be available!?
    argparser.add_argument("--md5", action="store_true",
        help="calculate and compare MD5 hashes",
    )
    argparser.add_argument("--sha1", action="store_true",
        help="calculate and compare SHA1 hashes",
    )
    argparser.add_argument("--sha224", action="store_true",
        help="calculate and compare SHA224 hashes",
    )
    argparser.add_argument("--sha256", action="store_true",
        help="calculate and compare SHA256 hashes",
    )
    argparser.add_argument("--sha384", action="store_true",
        help="calculate and compare SHA384 hashes",
    )
    argparser.add_argument("--sha512", action="store_true",
        help="calculate and compare SHA512 hashes",
    )
    argparser.add_argument("--algorithm", action="append",
        help="calculate and compare using this hash algorithm",
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
        logging.basicConfig(level=logging.DEBUG, format='[%(levelname)s] %(message)s',)

    # Check search directory
    dir = args.dir
    if not os.path.isdir(dir):
        raise Exception("search path not found: " + dir)

    # Get started
    debug("Initiating scanner for selected directory: %s" % (dir))
    opts = {}
    if args.ignore_vanished:
        opts["ignore-vanished"] = True
    if args.fatal_vanished:
        opts["fatal-vanished"] = True
    if args.md5:
        opts["algorithms"] = opts.get("algorithms", []) + ["MD5"]
    if args.sha1:
        opts["algorithms"] = opts.get("algorithms", []) + ["SHA1"]
    if args.sha224:
        opts["algorithms"] = opts.get("algorithms", []) + ["SHA224"]
    if args.sha256:
        opts["algorithms"] = opts.get("algorithms", []) + ["SHA256"]
    if args.sha384:
        opts["algorithms"] = opts.get("algorithms", []) + ["SHA384"]
    if args.sha512:
        opts["algorithms"] = opts.get("algorithms", []) + ["SHA512"]
    for algo_arg in args.algorithm or []:
        opts["algorithms"] = opts.get("algorithms", []) + [algo_arg.upper()]
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

    # Import/export map file
    if args.export_map_file:
        debug("Exporting map file: %s" % (args.export_map_file))
        with open(args.export_map_file, "w") as file:
            file.write(dupefilemap.json())

    # Export hashsums file ("-" == stdout)
    if args.export_hashsums_file:
        args.quiet = True
        debug("Exporting hashsums file: %s" % (args.export_hashsums_file))
        cur_algorithm = dupefilemap.default_algorithm()
        with file_open(args.export_hashsums_file, 'w') as f:
            for file_info in dupefilemap.list():
                cur_path = file_info["Path"]
                cur_hash = file_info[cur_algorithm]
                print("%s\t%s" % (cur_hash, cur_path), file=f)

    # Go through list of duplicate groups
    rc = 0
    duplicates_map = dupefilemap.duplicates_map()
    total_wasted_space = 0
    replaced_files = 0
    for hash, list in duplicates_map.items():
        # Duplicate group (group of identical files)
        group_wasted_space = 0
        if not args.quiet:
            print("[%s]" % (hash))
        for i, file_info in enumerate(list):
            # List file in group
            if not rc:
                rc = 2 # meaning at least one duplicate group found
            cur_path = file_info["Path"]
            if i == 0:
                if not args.quiet:
                    print("* %s" % (cur_path)) # first file in group
                src_path = cur_path
            else:
                group_wasted_space += file_info["Size"]
                if not args.quiet:
                    print("- %s" % (cur_path)) # (another) duplicate
                if args.link_duplicates:
                    # Replace duplicate with hardlink
                    # TODO this probably isn't the safest approach
                    # maybe create the link first, then remove the duplicate...
                    rc = 4 # meaning at least one duplicate replaced
                    debug("REPLACING file #%s: %s -> %s" % (file_info["Inum"], cur_path, src_path))
                    if not args.quiet:
                        print("    REPLACING #%s ..." % (file_info["Inum"]))
                    os.remove(cur_path)
                    os.link(src_path, cur_path)
                    replaced_files += 1
        total_wasted_space += group_wasted_space
        if not args.quiet:
            print()

    # Show summary
    # TODO format file size units
    if not (args.hide_summary or args.quiet):
        total_file_count = len(dupefilemap.list())
        total_files_size = sum(x["Size"] for x in dupefilemap.list())
        print("Files (scanned):\t%s" % (total_file_count))
        if dupefilemap.imported_hashed_files():
            print("Files (imported):\t%s" % (len(dupefilemap.imported_hashed_files())))
        print("Total size:\t\t%s B" % (total_files_size))
        print("Duplicate groups:\t%s" % (len(duplicates_map)))
        print("Total wasted space:\t%s B" % (total_wasted_space))
        print("Replaced files:\t\t%s" % (replaced_files))

    return rc


if __name__ == '__main__':
    exit(main())

