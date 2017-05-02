package main

import (
    "os"
    "syscall"
    "io/ioutil"
    "sync"
    "sort"
    "path/filepath"
    "encoding/json"
    "fmt"
)

type Scan struct {
    Paths []string
    Files FileMap
    HashFilesMap map[string]Files
    SortOrder int
    SortReversed bool
}

func NewScan() *Scan {
    scan := &Scan{}
    scan.Files = make(FileMap)

    return scan
}

func (scan *Scan) ImportMap(file string) error {
    //Read file
    raw, err := ioutil.ReadFile(file)
    if err != nil {
        return err
    }

    //Import map
    if err := json.Unmarshal(raw, &scan.Files); err != nil {
        return err
    }

    //Build hash files map
    scan.BuildHashFilesMap()

    return nil
}

func (scan *Scan) ExportMap(file string) error {
    //Export map
    json, err := json.Marshal(&scan.Files)
    if err != nil {
        return err
    }

    //Write file
    if err := ioutil.WriteFile(file, json, 0644); err != nil {
        return err
    }

    return nil
}

func (scan *Scan) ExportMD5(file string) error {
    //Export hash file
    f, err := os.Create(file)
    defer f.Close()
    if err != nil {
        return err
    }

    //Go thru files and get MD5 hash
    for _, file := range scan.Files {
        if file.RelativePath == "" {
            err := fmt.Errorf("no data generated for file, run scan")
            return err
        }
        if file.MD5 == "" {
            err := fmt.Errorf("no md5 hash generated for file: %s",
                file.RelativePath)
            return err
        }
        hashLine := file.MD5 + "  " + file.RelativePath
        _, err := f.WriteString(hashLine + "\n")
        if err != nil {
            return err
        }
    }

    //Sync/flush
    if err := f.Sync(); err != nil {
        return err
    }

    return nil
}

func (scan *Scan) Clean() FileList {
    var removedFiles FileList

    //Remove file objects that point to non-existent files
    for path, file := range scan.Files {
        if !file.Exists() {
            delete(scan.Files, path)
            removedFiles = append(removedFiles, file)
        }
    }

    //Rebuild hash files map
    scan.BuildHashFilesMap()

    return removedFiles
}

func (scan *Scan) Scan(wait *sync.WaitGroup) {
    go func() {
        defer wait.Done()

        //Remove non-existent files from list
        scan.Clean()

        //Scan search path recursively
        for _, path := range scan.Paths {
            //Search path (base)
            filepath.Walk(path, func(file string, fi os.FileInfo, err error) error {
                //Check for error
                if err != nil {
                    //Ignore errors (such as permission denied)
                    if fi.IsDir() {
                        return filepath.SkipDir
                    } else {
                        return err
                    }
                }

                //Directory
                if fi.IsDir() {
                    return err
                }

                //Regular file
                //Skip symlinks, a symlink target might be deleted as duplicate
                if fi.Mode().IsRegular() {
                    scan.scanFile(file, fi)
                }

                return nil
            })
        }

        //Rebuild hash files map
        scan.BuildHashFilesMap()

    }()
}

func (scan *Scan) scanFile(file string, fi os.FileInfo) error {
    //New file object
    fullPath, err := filepath.Abs(file)
    if err != nil {
        return err
    }
    newFile := &File{ RelativePath: file }
    newFile.Path = fullPath
    newFile.Name = fi.Name()
    newFile.Size = fi.Size()
    newFile.ModificationTime = fi.ModTime().Unix()

    //Get inode number, if possible
    if stat, ok := fi.Sys().(*syscall.Stat_t); ok {
        newFile.Inum = uint64(stat.Ino)
    }

    //Check for old file object
    oldFile, found := scan.Files[fullPath]
    if found && oldFile.IsHashed() {
        //File already in map, probably imported
        //Stat file, check size and time
        probablyIdentical := newFile.LooksIdentical(oldFile)
        if probablyIdentical {
            //Don't rescan file, keep it in map
            return nil
        }
    }

    //Hash
    if err := newFile.Hash(); err != nil {
        return err
    }

    //Add to map
    scan.Files[fullPath] = newFile

    return nil
}

func (scan *Scan) BuildHashFilesMap() map[string]Files {
    //Build hash map (hash -> file list)
    hashMap := make(map[string]Files)
    for _, file := range scan.Files {
        if !file.IsHashed() {
            //File not hashed, error
            continue
        }
        hash := file.HashValue()
        filesGroup := Files{
            sort: scan.SortOrder,
            reverse: scan.SortReversed,
        }
        if _, found := hashMap[hash]; !found {
            hashMap[hash] = filesGroup //new group
        } else {
            filesGroup = hashMap[hash] //incomplete list
        }
        filesGroup.Files = append(filesGroup.Files, file)
        hashMap[hash] = filesGroup //update list
    }

    //Sort
    for _, files := range hashMap {
        sort.Sort(files)
    }

    scan.HashFilesMap = hashMap
    return hashMap
}

func (scan *Scan) DuplicatesMap() map[string]FileList {
    duplicates := make(map[string]FileList)

    //Go through hash map (files grouped by hash)
    //Create map of duplicates, grouped by hash
    var addedInums []uint64
    for hash, files := range scan.HashFilesMap {
        fileList := files.Files //files with same hash
        var duplicateFiles FileList

        //Skip empty files
        if fileList[0].Size == 0 {
            continue
        }

        //Found hash with multiple files
        addedInums = nil
        FILES:
        for _, file := range fileList {
            if file.Inum != 0 {
                for _, otherInum := range addedInums {
                    if otherInum == file.Inum {
                        continue FILES
                    }
                }
                addedInums = append(addedInums, file.Inum)
            }
            duplicateFiles = append(duplicateFiles, file)
        }

        //Skip if only one file with current hash
        if len(duplicateFiles) == 1 {
            continue
        }

        //Add list of duplicates for current hash (identical files)
        duplicates[hash] = duplicateFiles

    }

    return duplicates
}

func (scan *Scan) AdditionalFilesMap() map[string]FileList {
    additional := make(map[string]FileList)

    for hash, files := range scan.DuplicatesMap() {
        additional[hash] = files[1:]
    }

    return additional
}

func (scan *Scan) AdditionalFiles() FileList {
    var additionalFiles FileList

    for _, files := range scan.AdditionalFilesMap() {
        additionalFiles = append(additionalFiles, files...)
    }

    return additionalFiles
}

func (scan *Scan) TotalFilesSize() int64 {
    var size int64
    for _, file := range scan.Files {
        size += file.Size
    }

    return size
}

func (scan *Scan) DuplicatesSize() int64 {
    var size int64

    //Sum file sizes of additional files (duplicates - 1 per group)
    //5 identical files (in group) = 4 additional files
    for _, files := range scan.AdditionalFilesMap() {
        duplicatesCount := len(files)
        var duplicatesSize int64
        duplicatesSize = files[0].Size * int64(duplicatesCount)
        size += duplicatesSize
    }

    return size
}

