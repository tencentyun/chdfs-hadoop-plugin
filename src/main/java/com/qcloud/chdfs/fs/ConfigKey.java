package com.qcloud.chdfs.fs;

public class ConfigKey {

    public static final String MOUNT_POINT_ADDR_PATTERN_CHDFS_TYPE =
            "^([a-zA-Z0-9-]+)\\.chdfs(-dualstack)?(\\.inner)?\\.([a-z0-9-]+)\\.([a-z0-9-.]+)";
    public static final String MOUNT_POINT_ADDR_PATTERN_COS_TYPE =
            "^([a-z0-9-]+)-([a-zA-Z0-9]+)$";
    public static final String CHDFS_USER_APPID_KEY = "fs.ofs.user.appid";
    public static final String CHDFS_TMP_CACHE_DIR_KEY = "fs.ofs.tmp.cache.dir";
    public static final String CHDFS_META_SERVER_PORT_KEY = "fs.ofs.meta.server.port";
    public static final String CHDFS_META_TRANSFER_USE_TLS_KEY = "fs.ofs.meta.transfer.tls";

    public static final String CHDFS_USE_L5_FLAG = "fs.ofs.use.l5.flag";

    public static final boolean DEFAULT_CHDFS_USE_L5_FLAG = false;

    public static final String CHDFS_META_USE_L5_ADDRESS = "fs.ofs.meta.l5.address";

    public static final String CHDFS_COS_USE_L5_ADDRESS = "fs.ofs.cos.l5.address";
    public static final String CHDFS_BUCKET_REGION = "fs.ofs.bucket.region";
    public static final String COS_ENDPOINT_SUFFIX = "fs.ofs.data.transfer.endpoint.suffix";

    public static final String CHDFS_META_ENDPOINT_SUFFIX_KEY = "fs.ofs.meta.endpoint.suffix";
    public static final boolean DEFAULT_CHDFS_META_TRANSFER_USE_TLS = true;
    public static final int DEFAULT_CHDFS_META_SERVER_PORT = 443;

    public static final String L5Separator = ",";

    public static final String CHDFS_DATA_TRANSFER_DISTINGUISH_HOST = "fs.ofs.data.transfer.distinguish.host";

    public static final boolean DEFAULT_CHDFS_DATA_TRANSFER_DISTINGUISH_FLAG = false;
}
