package com.qcloud.chdfs.fs;

import org.junit.Test;

import static org.junit.Assert.*;

public class CHDFSHadoopFileSystemAdapterTest {

    @Test
    public void testMountPointAddressCheck() {
        CHDFSHadoopFileSystemAdapter chdfsHadoopFileSystemAdapter = new CHDFSHadoopFileSystemAdapter();
        assertTrue(chdfsHadoopFileSystemAdapter.isValidMountPointAddr("xyz-f4m123.chdfs.ap-guangzhou.myqcloud.com"));
        assertTrue(chdfsHadoopFileSystemAdapter.isValidMountPointAddr("xyz-f4m123.chdfs.ap-guangzhou.test.xxx.yyy.zzz.com"));
    }
}