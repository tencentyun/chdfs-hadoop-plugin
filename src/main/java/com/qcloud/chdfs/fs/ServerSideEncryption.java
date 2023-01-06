package com.qcloud.chdfs.fs;

import java.io.IOException;

public interface ServerSideEncryption {
    void enableSseCos() throws IOException;
    void disableSse() throws IOException;
}