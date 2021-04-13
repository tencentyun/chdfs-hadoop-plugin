package com.qcloud.chdfs.fs;

import org.apache.hadoop.fs.FileSystem;

public abstract class FileSystemWithLockCleaner extends FileSystem implements FileLockCleaner {
}
