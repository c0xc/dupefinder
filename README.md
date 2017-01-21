DupeFinder
==========

This is a simple cli tool to find duplicates.

Duplicates are files with the same checksum, e.g., MD5 hash.

Duplicates are listed in groups.

It is possible to export a map file, which can be imported later to speed up
another scan of the same directory.
After importing a map file, files that have not changed
(same name, size, mtime) are not checksummed again.
This makes it possible to limit a subsequent scan to the changes
(new/modified files) rather than having to scan everything again
which may take hours to finish.



Author
------

Philip Seeger (philip@philip-seeger.de)



License
-------

Please see the file called LICENSE.



