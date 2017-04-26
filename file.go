package main

import (
    "os"
    "io"
    "encoding/hex"
    "crypto/md5"
    _ "crypto/sha1"
)

type File struct {
    Path string
    RelativePath string
    Name string
    Size int64
    ModificationTime int64
    MD5 string
    SHA1 string
    Inum uint64
}

type FileList []*File

type Files struct {
    Files FileList
    sort int
    reverse bool
}

func (f Files) Len() int {
    return len(f.Files)
}

func (f Files) Swap(i, j int) {
    f.Files[i], f.Files[j] = f.Files[j], f.Files[i]
}

func (f Files) Less(i, j int) bool {
    var l bool
    if f.sort == 0 {
        l = f.Files[i].Path < f.Files[j].Path
    } else if f.sort == 1 {
        l = f.Files[i].Name < f.Files[j].Name
    } else if f.sort == 2 {
        l = f.Files[i].Size < f.Files[j].Size
    } else if f.sort == 3 {
        l = f.Files[i].ModificationTime > f.Files[j].ModificationTime
    }
    if f.reverse {
        l = !l
    }
    return l
}

type FileMap map[string]*File

func (file *File) Exists() bool {
    fi, err := os.Stat(file.Path)
    exists := err == nil
    return exists && !fi.IsDir()
}

func (file *File) HashValue() string {
    var firstHash string

    if file.MD5 != "" {
        firstHash = file.MD5
    }
    if file.SHA1 != "" {
        firstHash = file.SHA1
    }

    return firstHash
}

func (file *File) IsHashed() bool {
    return file.HashValue() != ""
}

func (file *File) Hash() error {
    //Open file
    f, err := os.Open(file.Path)
    if err != nil {
        return err
    }
    defer f.Close()

    //MD5
    hashMD5 := md5.New()
    if _, err := io.Copy(hashMD5, f); err != nil {
        return err
    }
    file.MD5 = hex.EncodeToString(hashMD5.Sum(nil))

    return nil
}

func (file *File) LooksIdentical(other *File) bool {
    var probablyIdentical bool
    probablyIdentical = file.Path != ""

    //Compare size and mtime
    probablyIdentical = probablyIdentical &&
        file.Size == other.Size &&
        file.ModificationTime == other.ModificationTime

    return probablyIdentical
}

