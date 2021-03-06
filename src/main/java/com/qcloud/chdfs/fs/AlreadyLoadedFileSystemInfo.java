package com.qcloud.chdfs.fs;

import org.apache.hadoop.fs.FileSystem;

public class AlreadyLoadedFileSystemInfo {
    String versionId;
    String jarPath;
    String jarMd5;
    FileSystemWithLockCleaner actualFileSystem;

    public AlreadyLoadedFileSystemInfo(String versionId, String jarPath, String jarMd5, FileSystemWithLockCleaner actualFileSystem) {
        this.versionId = versionId;
        this.jarPath = jarPath;
        this.jarMd5 = jarMd5;
        this.actualFileSystem = actualFileSystem;
    }
}
