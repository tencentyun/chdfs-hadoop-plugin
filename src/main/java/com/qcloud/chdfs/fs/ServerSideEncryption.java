package com.qcloud.chdfs.fs;

import java.io.IOException;

public interface ServerSideEncryption {
    void enableSSECos() throws IOException;
    void disableSSE() throws IOException;
}