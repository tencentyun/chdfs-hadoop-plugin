package com.qcloud.chdfs.fs;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;

public class utils {
    public static void closeQuietly(InputStream input) {
        closeQuietly((Closeable)input);
    }

    public static void closeQuietly(Closeable closeable) {
        try {
            if (closeable != null) {
                closeable.close();
            }
        } catch (IOException ignore) {
        }
    }
}
