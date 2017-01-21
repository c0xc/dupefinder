package main

import (
    "fmt"
    "os"
    "flag"
    "sync"

    "github.com/dustin/go-humanize"
)

func main() {
    //Usage
    flag.Usage = func() {
        fmt.Printf("Usage:\n")
        fmt.Printf("\t%s [OPTION]... DIRECTORY\n", os.Args[0])
        fmt.Printf("\n")
        flag.PrintDefaults()
    }

    //Define arguments
    var mapFileImport string
    flag.StringVar(&mapFileImport, "map-file-import", "", "map file to import")
    var mapFileExport string
    flag.StringVar(&mapFileExport, "map-file-export", "", "map file to export")
    var mapFileReplace bool
    flag.BoolVar(&mapFileReplace, "map-file-replace", false,
        "replace file when exporting map")
    var listDuplicateGroups bool
    flag.BoolVar(&listDuplicateGroups, "list-duplicate-groups", true,
        "list duplicate groups")
    var listDuplicates bool
    flag.BoolVar(&listDuplicates, "list-duplicates", true,
        "list duplicate files (excluding first one)")
    var showSummary bool
    flag.BoolVar(&showSummary, "show-summary", true,
        "show summary of found duplicates")

    //Parse arguments
    flag.Parse()
    if flag.NArg() == 0 {
        flag.Usage()
        os.Exit(0)
    }

    //Wait lock
    var wait sync.WaitGroup

    //Scan object
    scan := NewScan()

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
    wait.Add(1)
    fmt.Fprintf(os.Stderr, "Scanning...\n")
    scan.Scan(&wait)
    wait.Wait()

    //Export file map
    if mapFileReplace && mapFileExport == "" {
        mapFileExport = mapFileImport
    }
    if mapFileExport != "" {
        if _, err := os.Stat(mapFileExport); err == nil {
            if !mapFileReplace {
                fmt.Fprintf(os.Stderr,
                    "Map file exists, not exporting: %s\n", mapFileExport)
                mapFileExport = ""
            }
        }
    }
    if mapFileExport != "" {
        if err := scan.ExportMap(mapFileExport); err != nil {
            fmt.Fprintf(os.Stderr,
                "Error exporting map: %s\n", err.Error())
            os.Exit(1)
        }
    }

    //List duplicate groups
    duplicatesMap := scan.DuplicatesMap()
    if listDuplicateGroups {
        for _, duplicates := range duplicatesMap {
            for _, file := range duplicates {
                fmt.Printf("%s\n", file.Path)
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
        fmt.Printf("Files:\t\t\t%d\n", totalFileCount)
        fmt.Printf("Total size:\t\t%s (%d B)\n",
            humanize.IBytes(totalFilesSize), totalFilesSize)
        fmt.Printf("Duplicate groups:\t%d\n", groupCount)
        fmt.Printf("Size of duplicates:\t%s (%d B)\n",
            humanize.IBytes(duplicatesSize), duplicatesSize)
        fmt.Printf("\n")
    }

}

