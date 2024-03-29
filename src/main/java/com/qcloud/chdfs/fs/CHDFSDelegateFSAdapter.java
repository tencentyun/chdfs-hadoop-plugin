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
}