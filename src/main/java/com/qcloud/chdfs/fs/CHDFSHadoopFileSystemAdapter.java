package com.qcloud.chdfs.fs;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CHDFSHadoopFileSystemAdapter extends FileSystem {
    private static final Logger log = LoggerFactory.getLogger(CHDFSHadoopFileSystemAdapter.class);

    static final String SCHEME = "chdfs";
    private static final String MOUNT_POINT_ADDR_PATTERN = "^([a-zA-Z0-9-]+)\\.chdfs(\\.inner)?\\.([a-z0-9-]+)\\.myqcloud\\.com$";
    private static final String CHDFS_USER_APPID_KEY = "fs.chdfs.user.appid";
    private static final String CHDFS_TMP_CACHE_DIR_KEY = "fs.chdfs.tmp.cache.dir";
    private static final String CHDFS_JAR_PLUGIN_SERVER_PORT_KEY = "fs.chdfs.jar.plugin.server.port";
    private static final int DEFAULT_CHDFS_JAR_PLUGIN_SERVER_PORT = 8080;

    @Override
    public String getScheme() {
        return CHDFSHadoopFileSystemAdapter.SCHEME;
    }

    private boolean isValidMountPointAddr(String mountPointAddr) {
        return Pattern.matches(MOUNT_POINT_ADDR_PATTERN, mountPointAddr);
    }

    private String initCacheTmpDir(Configuration conf) throws IOException {
        String chdfsTmpCacheDirPath = conf.get(CHDFS_TMP_CACHE_DIR_KEY);
        if (chdfsTmpCacheDirPath == null) {
            String errMsg = String.format("chdfs config %s is missing",
                    CHDFS_TMP_CACHE_DIR_KEY);
            log.error(errMsg);
            throw new IOException(errMsg);
        }
        if (!chdfsTmpCacheDirPath.startsWith("/")) {
            String errMsg = String.format("chdfs config [%s: %s] must be absolute path",
                    CHDFS_TMP_CACHE_DIR_KEY,
                    chdfsTmpCacheDirPath);
            log.error(errMsg);
            throw new IOException(errMsg);
        }

        File chdfsTmpCacheDir = new File(chdfsTmpCacheDirPath);
        if (!chdfsTmpCacheDir.exists()) {
            // judge exists again, may many map-reduce processes create cache dir parallel
            if (!chdfsTmpCacheDir.mkdirs() && !chdfsTmpCacheDir.exists()) {
                String errMsg = String.format("mkdir for chdfs tmp dir %s failed", chdfsTmpCacheDir.getAbsolutePath());
                log.error(errMsg);
                throw new IOException(errMsg);
            }
        }

        if (!chdfsTmpCacheDir.isDirectory()) {
            String errMsg = String.format("chdfs config [%s: %s] is invalid directory", CHDFS_TMP_CACHE_DIR_KEY, chdfsTmpCacheDir.getAbsolutePath());
            log.error(errMsg);
            throw new IOException(errMsg);
        }

        if (!chdfsTmpCacheDir.canRead()) {
            String errMsg = String.format("chdfs config [%s: %s] is not readable",
                    CHDFS_TMP_CACHE_DIR_KEY,
                    chdfsTmpCacheDirPath);
            log.error(errMsg);
            throw new IOException(errMsg);
        }

        if (!chdfsTmpCacheDir.canWrite()) {
            String errMsg = String.format("chdfs config [%s: %s] is not writeable",
                    CHDFS_TMP_CACHE_DIR_KEY,
                    chdfsTmpCacheDirPath);
            log.error(errMsg);
            throw new IOException(errMsg);
        }
        return chdfsTmpCacheDirPath;
    }

    private long getAppid(Configuration conf) throws IOException {
        long appid = 0;
        try {
            appid = conf.getLong(CHDFS_USER_APPID_KEY, 0);
        } catch (NumberFormatException e) {
            throw new IOException(String.format("config for %s is invalid appid number", CHDFS_USER_APPID_KEY));
        }
        if (appid <= 0) {
            throw new IOException(String.format("config for %s is missing or invalid appid number",
                    CHDFS_USER_APPID_KEY));
        }
        return appid;
    }

    private int getJarPluginServerPort(Configuration conf) {
        return conf.getInt(CHDFS_JAR_PLUGIN_SERVER_PORT_KEY, DEFAULT_CHDFS_JAR_PLUGIN_SERVER_PORT);
    }


    @Override
    public void initialize(URI name, Configuration conf) throws IOException {
        super.initialize(name, conf);

        String mountPointAddr = name.getHost();
        if (!isValidMountPointAddr(mountPointAddr)) {
            String errMsg = "mountPointAddr is invalid, exp. f4mabcdefgh-xyzw.chdfs.ap-guangzhou.myqcloud.com";
            log.error(errMsg);
            throw new IOException(errMsg);
        }

        long appid = getAppid(conf);
        int jarPluginServerPort = getJarPluginServerPort(conf);
        String tmpDirPath = initCacheTmpDir(conf);
        if (!CHDFSHadoopFileSystemJarLoader.INSTANCE.init(mountPointAddr, appid, jarPluginServerPort, tmpDirPath)) {
            String errMsg = "init chdfs impl failed";
            log.error(errMsg);
            throw new IOException(errMsg);
        } else {
            CHDFSHadoopFileSystemJarLoader.INSTANCE.getActualFileSystem().initialize(name, conf);
        }
    }


    @java.lang.Override
    public java.net.URI getUri() {
        return CHDFSHadoopFileSystemJarLoader.INSTANCE.getActualFileSystem().getUri();
    }

    @java.lang.Override
    public FSDataInputStream open(Path f, int bufferSize) throws IOException {
        return CHDFSHadoopFileSystemJarLoader.INSTANCE.getActualFileSystem().open(f, bufferSize);
    }

    @java.lang.Override
    public FSDataOutputStream create(Path f, FsPermission permission, boolean overwrite,
                                     int bufferSize, short replication, long blockSize, Progressable progress)
            throws IOException {
        return CHDFSHadoopFileSystemJarLoader.INSTANCE.getActualFileSystem()
                .create(f, permission, overwrite, bufferSize, replication, blockSize, progress);
    }

    @java.lang.Override
    public FSDataOutputStream append(Path f, int bufferSize, Progressable progress)
            throws IOException {
        return CHDFSHadoopFileSystemJarLoader.INSTANCE.getActualFileSystem()
                .append(f, bufferSize, progress);
    }

    @java.lang.Override
    public boolean rename(Path src, Path dst) throws IOException {
        return CHDFSHadoopFileSystemJarLoader.INSTANCE.getActualFileSystem()
                .rename(src, dst);
    }

    @java.lang.Override
    public boolean delete(Path f, boolean recursive) throws IOException {
        return CHDFSHadoopFileSystemJarLoader.INSTANCE.getActualFileSystem()
                .delete(f, recursive);
    }

    @java.lang.Override
    public FileStatus[] listStatus(Path f) throws FileNotFoundException, IOException {
        return CHDFSHadoopFileSystemJarLoader.INSTANCE.getActualFileSystem()
                .listStatus(f);
    }

    @java.lang.Override
    public void setWorkingDirectory(Path new_dir) {
        CHDFSHadoopFileSystemJarLoader.INSTANCE.getActualFileSystem()
                .setWorkingDirectory(new_dir);
    }

    @java.lang.Override
    public Path getWorkingDirectory() {
        return CHDFSHadoopFileSystemJarLoader.INSTANCE.getActualFileSystem()
                .getWorkingDirectory();
    }

    @java.lang.Override
    public boolean mkdirs(Path f, FsPermission permission) throws IOException {
        return CHDFSHadoopFileSystemJarLoader.INSTANCE.getActualFileSystem()
                .mkdirs(f, permission);
    }

    @java.lang.Override
    public FileStatus getFileStatus(Path f) throws IOException {
        return CHDFSHadoopFileSystemJarLoader.INSTANCE.getActualFileSystem()
                .getFileStatus(f);
    }

    @Override
    public void setPermission(Path p, FsPermission permission) throws IOException {
        CHDFSHadoopFileSystemJarLoader.INSTANCE.getActualFileSystem().setPermission(p, permission);
    }

    @Override
    public void setOwner(Path p, String username, String groupname) throws IOException {
        CHDFSHadoopFileSystemJarLoader.INSTANCE.getActualFileSystem()
                .setOwner(p, username, groupname);
    }

    @Override
    public void setTimes(Path p, long mtime, long atime) throws IOException {
        CHDFSHadoopFileSystemJarLoader.INSTANCE.getActualFileSystem()
                .setTimes(p, mtime, atime);
    }

    @Override
    public void close() throws IOException {
        CHDFSHadoopFileSystemJarLoader.INSTANCE.getActualFileSystem().close();
        super.close();
    }
}
