package com.qcloud.chdfs.fs;

import org.apache.hadoop.fs.FileSystem;

public class AlreadyLoadedFileSystemInfo {
    String versionId;
    String jarPath;
    String jarMd5;
    FileSystem actualFileSystem;

    public AlreadyLoadedFileSystemInfo(String versionId, String jarPath, String jarMd5, FileSystem actualFileSystem) {
        this.versionId = versionId;
        this.jarPath = jarPath;
        this.jarMd5 = jarMd5;
        this.actualFileSystem = actualFileSystem;
    }
}
