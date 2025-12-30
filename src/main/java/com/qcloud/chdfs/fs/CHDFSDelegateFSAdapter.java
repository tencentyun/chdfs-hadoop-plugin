package com.qcloud.chdfs.fs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.DelegateToFileSystem;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class CHDFSDelegateFSAdapter extends DelegateToFileSystem {

    public CHDFSDelegateFSAdapter(URI theUri, Configuration conf) throws IOException, URISyntaxException {
        super(theUri, new CHDFSHadoopFileSystemAdapter(), conf, CHDFSHadoopFileSystemAdapter.SCHEME, false);
    }

    @Override
    public int getUriDefaultPort() {
        return -1;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("CHDFSDelegateFSAdapter{");
        sb.append("URI =").append(fsImpl.getUri());
        sb.append("; fsImpl=").append(fsImpl);
        sb.append('}');
        return sb.toString();
    }

    /**
     * Close the file system; the FileContext API doesn't have an explicit close.
     */
    @Override
    protected void finalize() throws Throwable {
        fsImpl.close();
        super.finalize();
    }
}