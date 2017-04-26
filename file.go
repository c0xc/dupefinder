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
    Size int64
    ModificationTime int64
    MD5 string
    SHA1 string
    Inum uint64
}

type FileList []*File

func (list FileList) Len() int {
    return len(list)
}

func (list FileList) Swap(i, j int) {
    list[i], list[j] = list[j], list[i]
}

func (list FileList) Less(i, j int) bool {
    return list[i].ModificationTime < list[j].ModificationTime
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

