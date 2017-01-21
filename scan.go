package main

import (
    "os"
    "io/ioutil"
    "sync"
    "sort"
    "path/filepath"
    "encoding/json"
)

type Scan struct {
    Paths []string
    Files FileMap
    HashFilesMap map[string]FileList
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
                    }
                }

                //Scan file
                if !fi.IsDir() {
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
    newFile.Size = fi.Size()
    newFile.ModificationTime = fi.ModTime().Unix()

    //Check for old file object
    oldFile, found := scan.Files[fullPath]
    if found {
        if newFile.LooksIdentical(oldFile) && oldFile.IsHashed() {
            //Skip
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

func (scan *Scan) BuildHashFilesMap() map[string]FileList {
    //Build hash map (hash -> file list)
    hashMap := make(map[string]FileList)
    for _, file := range scan.Files {
        if !file.IsHashed() {
            //File not hashed, error
            continue
        }
        hash := file.HashValue()
        if _, found := hashMap[hash]; !found {
            hashMap[hash] = FileList{}
        }
        hashMap[hash] = append(hashMap[hash], file)
    }

    //Sort
    for _, files := range hashMap {
        sort.Sort(FileList(files))
    }

    scan.HashFilesMap = hashMap
    return hashMap
}

func (scan *Scan) DuplicatesMap() map[string]FileList {
    duplicates := make(map[string]FileList)

    //Go through hash map
    for hash, files := range scan.HashFilesMap {
        //Skip if only one file with current hash
        if len(files) == 1 {
            continue
        }

        //Skip empty files
        if files[0].Size == 0 {
            continue
        }

        //Found hash with multiple files
        duplicates[hash] = files
    }

    return duplicates
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

    //Sum file sizes of duplicates (excluding 1 file per group)
    for _, files := range scan.DuplicatesMap() {
        duplicatesCount := len(files) - 1 //5 identical files = 4 duplicates
        var duplicatesSize int64
        duplicatesSize = files[0].Size * int64(duplicatesCount)
        size += duplicatesSize
    }

    return size
}

