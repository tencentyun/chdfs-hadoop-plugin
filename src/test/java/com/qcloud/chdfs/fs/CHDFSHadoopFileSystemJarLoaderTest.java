package com.qcloud.chdfs.fs;

import org.junit.Test;

import static org.junit.Assert.*;

public class CHDFSHadoopFileSystemJarLoaderTest {

    @Test
    public void testModifyJarPath() {
        String originJarPath = "https://xxx-123.cos.ap-guangzhou.myqcloud.com";
        CHDFSHadoopFileSystemJarLoader jarLoader = new CHDFSHadoopFileSystemJarLoader(originJarPath);
        jarLoader.modifyJarPathAccordToEndpointSuffix();

        assertEquals(originJarPath, jarLoader.getJarPath());

        jarLoader.setChdfsDataTransferEndpointSuffix("test.com");
        jarLoader.modifyJarPathAccordToEndpointSuffix();
        assertEquals("http://xxx-123.test.com", jarLoader.getJarPath());
    }
}