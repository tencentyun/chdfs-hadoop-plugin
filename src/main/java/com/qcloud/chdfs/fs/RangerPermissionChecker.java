package com.qcloud.chdfs.fs;

import com.qcloud.chdfs.permission.RangerAccessType;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public interface RangerPermissionChecker {
    void checkPermission(Path f, RangerAccessType rangerAccessType) throws IOException;
}
