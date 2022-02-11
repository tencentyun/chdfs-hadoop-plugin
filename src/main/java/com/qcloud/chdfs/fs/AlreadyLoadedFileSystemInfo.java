package com.qcloud.chdfs.fs;

public class AlreadyLoadedFileSystemInfo {
    String versionId;
    String jarPath;
    String jarMd5;
    Class chdfsFSClass;

    public AlreadyLoadedFileSystemInfo(String versionId, String jarPath, String jarMd5,
            Class chdfsFSClass) {
        this.versionId = versionId;
        this.jarPath = jarPath;
        this.jarMd5 = jarMd5;
        this.chdfsFSClass = chdfsFSClass;
    }
}
