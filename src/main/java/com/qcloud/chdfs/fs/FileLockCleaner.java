package com.qcloud.chdfs.fs;

import org.apache.hadoop.fs.Path;

import java.io.IOException;

public interface FileLockCleaner {
    void releaseFileLock(Path p) throws IOException;
}