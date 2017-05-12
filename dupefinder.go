package main

import (
    "fmt"
    "os"
    "flag"
    "sync"
    "path/filepath"
    "io"
    "io/ioutil"
    "bytes"
    "runtime"

    "github.com/dustin/go-humanize"
)

var verboseIO io.Writer

func main() {
    //Usage
    flag.Usage = func() {
        fmt.Printf("Usage:\n")
        fmt.Printf("\t%s [OPTION]... DIRECTORY\n", os.Args[0])
        fmt.Printf("\n")
        flag.PrintDefaults()
        fmt.Printf("\n")
        fmt.Printf("DupeFinder will first scan the specified directory. This process will take a long time. Unless a map file is provided, all files will be hashed. It is highly recommended to create a map file (-export-map-file FILE) if you're going to scan the same directory again. A map file contains the scan results, which can be imported (-import-map-file FILE) to reuse those results rather than doing another full scan. That way, only new or changed files will be hashed, so the second scan should take much less time. However, the size and time of all files will still be compared during the superficial scan (sanity check).\n")
        fmt.Printf("\n")
        fmt.Printf("If you're running DupeFinder on the same exact directory again (using the same path argument as before), you have the option to skip the scan that would do a sanity check on the imported map data (-skip-scan). This will make the second run take even less time. It will immediately start removing duplicates if you've told it to do so. All the paths must be identical. If you're using a different directory path when specifying this option than you did when exporting the map, the program might delete the wrong files. If you're using this option to skip the scan, you should not have the program remove duplicate files unless you know what you are doing.\n")
        fmt.Printf("If you've not specified an action, it will immediately print the summary. If you've specified a hash file to be created, it will merely copy the contents of the imported map.\n")
        fmt.Printf("\n")
        fmt.Printf("After the scan has completed, DupeFinder has a map in memory, representing the contents of the scanned directory. It will list all the duplicate groups, i.e., identical files (same hash) grouped together (-list-duplicate-groups). These groups are sorted by path (-sort-path), file name (-sort-name) or modification time, newest first (-sort-time). If the program is told to get rid of the duplicates, it will keep the first file of each group.\n")
        fmt.Printf("\n")
        fmt.Printf("To delete duplicate files, use -delete-duplicates. Be careful. You should first run the program without this option and make sure that all additional files (all files except the first one in each group) can be deleted. Then run the program again, with -delete-duplicates. You should also export a map file the first time you run it and import it the second time to prevent it from scanning everything again.\n")
        fmt.Printf("\n")
        fmt.Printf("An alternative to deleting additional identical files is linking them together. This means that all but one file of each group are replaced with hardlinks pointing to the first file. This would reduce the space wasted by all those duplicates to zero (this number is shown in the summary), but the drawback is that all files in the group would be affected if you later decide to change one of them because they all point to the same data. For archive systems or other kinds of collections with files that are never changed, this should be the ideal solution to save disk space.\n")
        fmt.Printf("\n")
    }

    //Define arguments
    var mapFileImport string
    flag.StringVar(&mapFileImport, "import-map-file", "", "map file to import, imported files won't be hashed (superficial scan)")
    var mapFileExport string
    flag.StringVar(&mapFileExport, "export-map-file", "", "map file to export")
    var exportFileReplace bool
    flag.BoolVar(&exportFileReplace, "file-replace", false,
        "replace file when exporting file")
    var hashMD5FileExport string
    flag.StringVar(&hashMD5FileExport, "export-md5sums-file", "", "export MD5SUMS file")
    var skipScan bool
    flag.BoolVar(&skipScan, "skip-scan", false,
        "skip scan when map is provided instead of doing superficial scan")
    var listDuplicateGroups bool
    flag.BoolVar(&listDuplicateGroups, "list-duplicate-groups", true,
        "list duplicate groups")
    var showSummary bool
    flag.BoolVar(&showSummary, "show-summary", true,
        "show summary of found duplicates")
    var deleteDuplicates bool
    flag.BoolVar(&deleteDuplicates, "delete-duplicates", false,
        "delete duplicates (keep first file per group)")
    var linkDuplicates bool
    flag.BoolVar(&linkDuplicates, "link-duplicates", false,
        "replace duplicates with hardlinks")
    var sortReversed bool
    flag.BoolVar(&sortReversed, "sort-reversed", false,
        "show duplicate groups in reversed order")
    var sortPath bool
    flag.BoolVar(&sortPath, "sort-path", true,
        "sort duplicate groups by file path")
    var sortName bool
    flag.BoolVar(&sortName, "sort-name", false,
        "sort duplicate groups by file name")
    var sortTime bool
    flag.BoolVar(&sortTime, "sort-time", false,
        "sort duplicate groups by file time")
    var useFullPath bool
    flag.BoolVar(&useFullPath, "use-full-path", false,
        "use absolute instead of relative path for scanned files")
    var verboseMode bool
    flag.BoolVar(&verboseMode, "verbose", false,
        "verbose output")
    var workerCount int
    flag.IntVar(&workerCount, "worker-count", runtime.NumCPU(),
        "number of scan workers, how many files to process in parallel")

    //Parse arguments
    flag.Parse()
    if flag.NArg() == 0 {
        flag.Usage()
        os.Exit(0)
    }

    //Verbose output
    verboseIO = bytes.NewBufferString("")
    if verboseMode {
        verboseIO = os.Stderr
    }

    //Helper for file path
    //File paths should be relative, so that a mounted network share
    //can be scanned using a map file created on the remote host.
    filePath := func(file *File) string {
        var path string
        if useFullPath {
            path = file.FullPath
        } else {
            path = file.Path
        }
        return path
    }

    //Wait lock
    var wait sync.WaitGroup

    //Scan object
    scan := NewScan()
    if sortPath {
        scan.SortOrder = 0
    }
    if sortName {
        scan.SortOrder = 1
    }
    if sortTime {
        scan.SortOrder = 3
    }
    scan.SortReversed = sortReversed
    scan.WorkerCount = workerCount

    //Search path
    for _, path := range flag.Args() {
        //Check if path exists
        var stat os.FileInfo
        stat, err := os.Stat(path)
        if err != nil {
            fmt.Fprintf(os.Stderr, "%s\n", err)
            os.Exit(1)
        }

        //Check if path is a directory
        if !stat.IsDir() {
            fmt.Fprintf(os.Stderr, "Not a directory: %s\n", path)
            os.Exit(1)
        }

        //Add path to list
        scan.Paths = append(scan.Paths, path)
    }

    //Search path must be defined
    if len(scan.Paths) == 0 {
        fmt.Fprintf(os.Stderr, "No search path defined\n")
        os.Exit(1)
    }

    //Check for file conflict (map file)
    if mapFileExport != "" {
        //User wants to create a map file
        if _, err := os.Stat(mapFileExport); err == nil {
            //Specified file already exists
            if !exportFileReplace {
                //User didn't confirm that file should be replaced
                fmt.Fprintf(os.Stderr,
                    "Not exporting map file, file exists, use -file-replace to override: %s\n", mapFileExport)
                mapFileExport = ""
                os.Exit(1)
            }
        }
    }
    if hashMD5FileExport != "" {
        //User wants to export a hash file
        if _, err := os.Stat(hashMD5FileExport); err == nil {
            //Specified file already exists
            if !exportFileReplace {
                //User didn't confirm that file should be replaced
                fmt.Fprintf(os.Stderr,
                    "Not exporting hash file, file exists, use -file-replace to override: %s\n", hashMD5FileExport)
                hashMD5FileExport = ""
                os.Exit(1)
            }
        }
    }

    //Import file map
    if mapFileImport != "" {
        if _, err := os.Stat(mapFileImport); err != nil {
            fmt.Fprintf(os.Stderr, "Map file not found: %s\n", mapFileImport)
            os.Exit(1)
        }
        if err := scan.ImportMap(mapFileImport); err != nil {
            fmt.Fprintf(os.Stderr,
                "Error importing map: %s\n", err.Error())
            os.Exit(1)
        }
        fmt.Fprintf(os.Stderr, "Imported files: %d\n", len(scan.Files))
    }

    //Start scan
    if (skipScan) {
        fmt.Println("Skipping scan")
    } else {
        wait.Add(1)
        fmt.Fprintf(os.Stderr, "Scanning...\n")
        fmt.Fprintf(os.Stderr, "\n")
        scan.Scan(&wait)
        wait.Wait()
    }

    //Export file map
    if exportFileReplace && mapFileExport == "" {
        mapFileExport = mapFileImport
    }
    if mapFileExport != "" {
        if err := scan.ExportMap(mapFileExport); err != nil {
            fmt.Fprintf(os.Stderr,
                "Error exporting map: %s\n", err.Error())
            os.Exit(1)
        }
    }

    //Export hash file
    if hashMD5FileExport != "" {
        if err := scan.ExportMD5(hashMD5FileExport); err != nil {
            fmt.Fprintf(os.Stderr,
                "Error exporting hash file: %s\n", err.Error())
            os.Exit(1)
        }
    }

    //List duplicate groups
    duplicatesMap := scan.DuplicatesMap()
    if listDuplicateGroups {
        for _, files := range duplicatesMap {
            for _, file := range files {
                fmt.Printf("%s\n", filePath(file))
            }
            fmt.Printf("\n")
        }
    }

    //Show summary
    if showSummary {
        totalFileCount := len(scan.Files)
        totalFilesSize := uint64(scan.TotalFilesSize())
        groupCount := len(duplicatesMap)
        duplicatesSize := uint64(scan.DuplicatesSize())
        var duplicateCount int
        duplicateCount = len(scan.AdditionalFiles())
        fmt.Printf("Files:\t\t\t%d\n", totalFileCount)
        fmt.Printf("Total size:\t\t%s (%d B)\n",
            humanize.IBytes(totalFilesSize), totalFilesSize)
        fmt.Printf("Duplicate groups:\t%d\n", groupCount)
        fmt.Printf("Duplicate count:\t%d\n", duplicateCount)
        fmt.Printf("Size of duplicates:\t%s (%d B)\n",
            humanize.IBytes(duplicatesSize), duplicatesSize)
        fmt.Printf("\n")
    }

    //Action
    if deleteDuplicates {
        //Delete duplicates (keep first one per group)

        for _, files := range duplicatesMap {
            duplicates := files[1:] //except first one
            for _, file := range duplicates {
                path := filePath(file)
                err := os.Remove(path)
                if err != nil {
                    fmt.Fprintf(os.Stderr,
                        "Error deleting file %s: %s\n", path, err.Error())
                    continue
                }
                fmt.Printf("Deleted %s\n", path)
            }
        }
    } else if linkDuplicates {
        //Replace duplicates with links
        //We assume they're all on the same filesystem

        for _, files := range duplicatesMap {
            firstFile := files[0]
            duplicates := files[1:] //except first one
            for _, file := range duplicates {
                //Create hardlink in destination directory
                //Replace duplicate only if hardlink created successfully
                duplicateFilePath := filePath(file)
                firstFilePath := filePath(firstFile)
                dir := filepath.Dir(duplicateFilePath) //hardlink directory
                prefix := "DUPE"
                f, err := ioutil.TempFile(dir, prefix)
                if err != nil {
                    fmt.Fprintf(os.Stderr,
                        "Error writing to directory %s: %s\n",
                        dir, err.Error())
                    continue
                }
                tmpFilePath := f.Name()
                f.Close()
                os.Remove(tmpFilePath)

                //Create hardlink using temporary (new) file
                //Fails if duplicate is on another filesystem
                if err := os.Link(firstFilePath, tmpFilePath); err != nil {
                    fmt.Fprintf(os.Stderr,
                        "Error creating link: %s\n",
                        err.Error())
                    continue
                }

                //Replace duplicate with link
                if err := os.Rename(tmpFilePath, duplicateFilePath); err != nil {
                    fmt.Fprintf(os.Stderr,
                        "Error replacing file %s with link: %s\n",
                        duplicateFilePath, err.Error())
                    continue
                }
                fmt.Printf("Replaced %s\n", duplicateFilePath)
            }
        }
    }

}

