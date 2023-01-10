package com.qcloud.chdfs.fs;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FsStatus;
import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CHDFSHadoopFileSystemAdapter extends FileSystemWithLockCleaner {
    static final String SCHEME = "ofs";
    private static final Logger log = LoggerFactory.getLogger(CHDFSHadoopFileSystemAdapter.class);


    private final CHDFSHadoopFileSystemJarLoader jarLoader = new CHDFSHadoopFileSystemJarLoader();
    private FileSystemWithLockCleaner actualImplFS = null;
    private URI uri = null;
    private Path workingDir = null;
    private long initStartMs;

    @Override
    public String getScheme() {
        return CHDFSHadoopFileSystemAdapter.SCHEME;
    }

    @Override
    public void initialize(URI name, Configuration conf) throws IOException {
        log.debug("CHDFSHadoopFileSystemAdapter adapter initialize");
        this.initStartMs = System.currentTimeMillis();
        log.debug("CHDFSHadoopFileSystemAdapter start-init-start time: {}", initStartMs);
        try {
            super.initialize(name, conf);
            this.setConf(conf);
            String mountPointAddr = name.getHost();
            if (mountPointAddr == null) {
                String errMsg = String.format("mountPointAddr is null, fullUri: %s, exp. f4mabcdefgh-xyzw.chdfs"
                        + ".ap-guangzhou.myqcloud.com or examplebucket-1250000000 or f4mabcdefgh-xyzw", name.toString());
                log.error(errMsg);
                throw new IOException(errMsg);
            }

            String ofsHost;
            if (isValidMountPointAddrChdfsType(mountPointAddr)) {
                ofsHost = mountPointAddr;
            } else if (isValidMountPointAddrCosType(mountPointAddr)) {
                String metaEndpointSuffix = getMetaEndpointSuffix(conf);
                if (!metaEndpointSuffix.isEmpty()) {
                    ofsHost = mountPointAddr + "." + metaEndpointSuffix;
                    // force close tls
                    conf.setBoolean(ConfigKey.CHDFS_META_TRANSFER_USE_TLS_KEY, false);
                } else {
                    String bucketRegion = getChdfsBucketRegion(conf);
                    ofsHost = String.format("%s.chdfs.%s.myqcloud.com", mountPointAddr, bucketRegion);
                }
            } else {
                String errMsg = String.format("mountPointAddr %s is invalid, fullUri: %s, exp. f4mabcdefgh-xyzw.chdfs"
                                + ".ap-guangzhou.myqcloud.com or examplebucket-1250000000 or f4mabcdefgh-xyzw",
                        mountPointAddr, name.toString());
                log.error(errMsg);
                throw new IOException(errMsg);
            }

            String networkVersionId = initPluginNetworkVersion();
            conf.set("chdfs.hadoop.plugin.network.version", String.format("network:%s", networkVersionId));

            long appid = getAppid(conf);
            int jarPluginServerPort = getJarPluginServerPort(conf);
            String tmpDirPath = initCacheTmpDir(conf);
            boolean jarPluginServerHttpsFlag = isJarPluginServerHttps(conf);

            boolean useL5 = isUseL5(conf);
            String metaL5Address = "";
            String cosL5Address = "";
            if (useL5) {
                metaL5Address = getMetaL5Address(conf);
                cosL5Address = getCosL5Address(conf);
            }

            String cosEndPointSuffix = getCosEndPointSuffix(conf);
            boolean distinguishHost = isDistinguishHost(conf);
            log.debug("fs.ofs.data.transfer.distinguish.host: {}", distinguishHost);
            initJarLoadWithRetry(ofsHost, appid, jarPluginServerPort, tmpDirPath, jarPluginServerHttpsFlag,
                    cosEndPointSuffix, distinguishHost, networkVersionId, useL5, metaL5Address, cosL5Address);

            this.actualImplFS = jarLoader.getActualFileSystem();
            if (this.actualImplFS == null) {
                // should never reach here
                throw new IOException("impl filesystem is null");
            }

            long actualInitStartMs = System.currentTimeMillis();
            this.actualImplFS.initialize(name, conf);
            log.debug("init actual file system, [elapse-ms: {}]", System.currentTimeMillis() - actualInitStartMs);
            this.uri = this.actualImplFS.getUri();
            this.workingDir = this.actualImplFS.getWorkingDirectory();
        } catch (IOException ioe) {
            log.error("initialize failed! a ioException occur!", ioe);
            throw ioe;
        } catch (Exception e) {
            log.error("initialize failed! a unexpected exception occur!", e);
            throw new IOException("initialize failed! oops! a unexpected exception occur! " + e.toString(), e);
        }
        log.debug("total init file system, [elapse-ms: {}]", System.currentTimeMillis() - initStartMs);
    }

    boolean isValidMountPointAddrChdfsType(String mountPointAddr) {
        return Pattern.matches(ConfigKey.MOUNT_POINT_ADDR_PATTERN_CHDFS_TYPE, mountPointAddr);
    }

    boolean isValidMountPointAddrCosType(String mountPointAddr) {
        return Pattern.matches(ConfigKey.MOUNT_POINT_ADDR_PATTERN_COS_TYPE, mountPointAddr);
    }

    private String getCosEndPointSuffix(Configuration conf) throws IOException {
        return conf.get(ConfigKey.COS_ENDPOINT_SUFFIX);
    }

    private String getChdfsBucketRegion(Configuration conf) throws IOException {
        String bucketRegion = conf.get(ConfigKey.CHDFS_BUCKET_REGION);
        if (bucketRegion == null) {
            String errMsg = String.format("ofs config %s is missing", ConfigKey.CHDFS_BUCKET_REGION);
            log.error(errMsg);
            throw new IOException(errMsg);
        }
        return bucketRegion;
    }

    private String getMetaEndpointSuffix(Configuration conf) throws IOException {
        return initStringValue(conf, ConfigKey.CHDFS_META_ENDPOINT_SUFFIX_KEY, "").toLowerCase();
    }

    private String initStringValue(Configuration conf, String configKey, String defaultValue)
            throws IOException {
        String retValue = conf.get(configKey);
        if (retValue == null) {
            if (defaultValue == null) {
                String errMsg = String.format("chdfs config %s missing", configKey);
                log.error(errMsg);
                throw new IOException(errMsg);
            } else {
                retValue = defaultValue;
                log.debug("chdfs config {} missing, use default value {}", configKey, defaultValue);
            }
        } else {
            if (retValue.trim().isEmpty()) {
                String errMsg = String.format("chdfs config %s value %s is invalid, value should not be empty string",
                        configKey, retValue);
                log.error(errMsg);
                throw new IOException(errMsg);
            }
        }
        return retValue.trim();
    }

    private long getAppid(Configuration conf) throws IOException {
        long appid = 0;
        try {
            appid = conf.getLong(ConfigKey.CHDFS_USER_APPID_KEY, 0);
        } catch (NumberFormatException e) {
            throw new IOException(String.format("config for %s is invalid appid number", ConfigKey.CHDFS_USER_APPID_KEY));
        }
        if (appid <= 0) {
            throw new IOException(
                    String.format("config for %s is missing or invalid appid number", ConfigKey.CHDFS_USER_APPID_KEY));
        }
        return appid;
    }

    private int getJarPluginServerPort(Configuration conf) {
        return conf.getInt(ConfigKey.CHDFS_META_SERVER_PORT_KEY, ConfigKey.DEFAULT_CHDFS_META_SERVER_PORT);
    }

    private String getMetaL5Address(Configuration conf) throws IOException {
        String l5Address = conf.get(ConfigKey.CHDFS_META_USE_L5_ADDRESS);
        if (l5Address == null) {
            throw new IOException(
                    String.format("config for %s is missing", ConfigKey.CHDFS_META_USE_L5_ADDRESS));
        }
        String[] address = l5Address.split(ConfigKey.L5Separator);
        if (address.length != 2) {
            throw new IOException(
                    String.format("config for %s invalid l5 address", ConfigKey.CHDFS_META_USE_L5_ADDRESS));
        }
        return l5Address;
    }

    private String getCosL5Address(Configuration conf) throws IOException {
        String l5Address = conf.get(ConfigKey.CHDFS_COS_USE_L5_ADDRESS);
        if (l5Address == null) {
            throw new IOException(
                    String.format("config for %s is missing", ConfigKey.CHDFS_COS_USE_L5_ADDRESS));
        }
        String[] address = l5Address.split(ConfigKey.L5Separator);
        if (address.length != 2) {
            throw new IOException(
                    String.format("config for %s invalid l5 address", ConfigKey.CHDFS_COS_USE_L5_ADDRESS));
        }
        return l5Address;
    }

    private String initCacheTmpDir(Configuration conf) throws IOException {
        String chdfsTmpCacheDirPath = conf.get(ConfigKey.CHDFS_TMP_CACHE_DIR_KEY);
        if (chdfsTmpCacheDirPath == null) {
            String errMsg = String.format("chdfs config %s is missing", ConfigKey.CHDFS_TMP_CACHE_DIR_KEY);
            log.error(errMsg);
            throw new IOException(errMsg);
        }
        if (!chdfsTmpCacheDirPath.startsWith("/")) {
            String errMsg = String.format("chdfs config [%s: %s] must be absolute path", ConfigKey.CHDFS_TMP_CACHE_DIR_KEY,
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
            chdfsTmpCacheDir.setReadable(true, false);
            chdfsTmpCacheDir.setWritable(true, false);
            chdfsTmpCacheDir.setExecutable(true, false);
        }

        if (!chdfsTmpCacheDir.isDirectory()) {
            String errMsg = String.format("chdfs config [%s: %s] is invalid directory", ConfigKey.CHDFS_TMP_CACHE_DIR_KEY,
                    chdfsTmpCacheDir.getAbsolutePath());
            log.error(errMsg);
            throw new IOException(errMsg);
        }

        if (!chdfsTmpCacheDir.canRead()) {
            String errMsg = String.format("chdfs config [%s: %s] is not readable", ConfigKey.CHDFS_TMP_CACHE_DIR_KEY,
                    chdfsTmpCacheDirPath);
            log.error(errMsg);
            throw new IOException(errMsg);
        }

        if (!chdfsTmpCacheDir.canWrite()) {
            String errMsg = String.format("chdfs config [%s: %s] is not writeable", ConfigKey.CHDFS_TMP_CACHE_DIR_KEY,
                    chdfsTmpCacheDirPath);
            log.error(errMsg);
            throw new IOException(errMsg);
        }
        return chdfsTmpCacheDirPath;
    }

    private boolean isJarPluginServerHttps(Configuration conf) {
        return conf.getBoolean(ConfigKey.CHDFS_META_TRANSFER_USE_TLS_KEY, ConfigKey.DEFAULT_CHDFS_META_TRANSFER_USE_TLS);
    }

    private boolean isUseL5(Configuration conf) {
        return conf.getBoolean(ConfigKey.CHDFS_USE_L5_FLAG, ConfigKey.DEFAULT_CHDFS_USE_L5_FLAG);
    }

    private boolean isDistinguishHost(Configuration conf) {
        return conf.getBoolean(ConfigKey.CHDFS_DATA_TRANSFER_DISTINGUISH_HOST, ConfigKey.DEFAULT_CHDFS_DATA_TRANSFER_DISTINGUISH_FLAG);
    }


    private void initJarLoadWithRetry(String mountPointAddr, long appid, int jarPluginServerPort, String tmpDirPath,
                                      boolean jarPluginServerHttps, String cosEndPointSuffix, boolean distinguishHost
            , String networkVersionId, boolean useL5, String metaL5Address, String cosL5Address) throws IOException {
        int maxRetry = 5;
        for (int retryIndex = 0; retryIndex <= maxRetry; retryIndex++) {
            try {
                jarLoader.init(mountPointAddr, appid, jarPluginServerPort, tmpDirPath, jarPluginServerHttps,
                        cosEndPointSuffix, distinguishHost, networkVersionId, useL5, metaL5Address, cosL5Address);
                return;
            } catch (IOException e) {
                if (retryIndex < maxRetry) {
                    log.warn(String.format("init chdfs impl failed, we will retry again, retryInfo: %d/%d", retryIndex,
                            maxRetry), e);
                } else {
                    log.error("init chdfs impl failed", e);
                    throw new IOException("init chdfs impl failed", e);
                }
            }
            try {
                Thread.sleep(ThreadLocalRandom.current().nextLong(500, 2000));
            } catch (InterruptedException ignore) {
                // ignore
            }
        }
    }

    @java.lang.Override
    public java.net.URI getUri() {
        return this.uri;
    }

    private void judgeActualFSInitialized() {
        if (this.actualImplFS == null) {
            throw new RuntimeException("please init the fileSystem first!");
        }
    }

    private String initPluginNetworkVersion() {
        String networkVersionId = "unknown";

        Properties versionProp = new Properties();
        InputStream in = null;
        final String versionPropName = "chdfsHadoopPluginNetworkVersionInfo.properties";
        try {
            in = this.getClass().getClassLoader().getResourceAsStream(versionPropName);
            if (in != null) {
                versionProp.load(in);
                networkVersionId = versionProp.getProperty("network_version");

            } else {
                log.error("load versionInfo properties failed, propName: {} ", versionPropName);
            }
        } catch (IOException e) {
            log.error("load versionInfo properties failed", e);
        } finally {
            IOUtils.closeQuietly(in);
        }
        return networkVersionId;
    }

    @java.lang.Override
    public FSDataInputStream open(Path f, int bufferSize) throws IOException {
        judgeActualFSInitialized();
        return this.actualImplFS.open(f, bufferSize);
    }

    @java.lang.Override
    public FSDataOutputStream createNonRecursive(Path f, FsPermission permission, EnumSet<CreateFlag> flags,
                                                 int bufferSize, short replication, long blockSize, Progressable progress) throws IOException {
        judgeActualFSInitialized();
        return this.actualImplFS.createNonRecursive(f, permission, flags, bufferSize, replication, blockSize, progress);
    }

    @java.lang.Override
    public FSDataOutputStream create(Path f, FsPermission permission, boolean overwrite, int bufferSize,
                                     short replication, long blockSize, Progressable progress) throws IOException {
        judgeActualFSInitialized();
        return this.actualImplFS.create(f, permission, overwrite, bufferSize, replication, blockSize, progress);
    }

    @java.lang.Override
    public FSDataOutputStream append(Path f, int bufferSize, Progressable progress) throws IOException {
        judgeActualFSInitialized();
        return this.actualImplFS.append(f, bufferSize, progress);
    }

    public boolean truncate(Path f, long newLength) throws IOException {
        judgeActualFSInitialized();
        return this.actualImplFS.truncate(f, newLength);
    }

    @Override
    public void concat(Path trg, Path[] psrcs) throws IOException {
        judgeActualFSInitialized();
        this.actualImplFS.concat(trg, psrcs);
    }

    @java.lang.Override
    public boolean rename(Path src, Path dst) throws IOException {
        judgeActualFSInitialized();
        return this.actualImplFS.rename(src, dst);
    }

    @java.lang.Override
    public boolean delete(Path f, boolean recursive) throws IOException {
        judgeActualFSInitialized();
        return this.actualImplFS.delete(f, recursive);
    }

    @java.lang.Override
    public boolean deleteOnExit(Path f) throws IOException {
        judgeActualFSInitialized();
        return this.actualImplFS.deleteOnExit(f);
    }

    @java.lang.Override
    public boolean cancelDeleteOnExit(Path f) {
        judgeActualFSInitialized();
        return this.actualImplFS.cancelDeleteOnExit(f);
    }


    @java.lang.Override
    public FileStatus[] listStatus(Path f) throws FileNotFoundException, IOException {
        judgeActualFSInitialized();
        return this.actualImplFS.listStatus(f);
    }

    @java.lang.Override
    public Path getWorkingDirectory() {
        return this.workingDir;
    }

    @java.lang.Override
    public void setWorkingDirectory(Path new_dir) {
        this.workingDir = new_dir;
        if (this.actualImplFS == null) {
            log.warn("fileSystem is not init yet!");
        } else {
            this.actualImplFS.setWorkingDirectory(new_dir);
        }
    }

    @Override
    public Path getHomeDirectory() {
        if (this.actualImplFS == null) {
            return super.getHomeDirectory();
        }
        return this.actualImplFS.getHomeDirectory();
    }

    @java.lang.Override
    public boolean mkdirs(Path f, FsPermission permission) throws IOException {
        judgeActualFSInitialized();
        return this.actualImplFS.mkdirs(f, permission);
    }

    @java.lang.Override
    public FileStatus getFileStatus(Path f) throws IOException {
        judgeActualFSInitialized();
        return this.actualImplFS.getFileStatus(f);
    }

    @Override
    public void createSymlink(Path target, Path link, boolean createParent)
            throws AccessControlException, FileAlreadyExistsException, FileNotFoundException,
            ParentNotDirectoryException, UnsupportedFileSystemException, IOException {
        judgeActualFSInitialized();
        this.actualImplFS.createSymlink(target, link, createParent);
    }

    @Override
    public FileStatus getFileLinkStatus(final Path f)
            throws AccessControlException, FileNotFoundException, UnsupportedFileSystemException, IOException {
        judgeActualFSInitialized();
        return this.actualImplFS.getFileLinkStatus(f);
    }

    @Override
    public boolean supportsSymlinks() {
        if (this.actualImplFS == null) {
            return super.supportsSymlinks();
        }
        return this.actualImplFS.supportsSymlinks();
    }


    @Override
    public Path getLinkTarget(Path f) throws IOException {
        judgeActualFSInitialized();
        return this.actualImplFS.getLinkTarget(f);
    }

    @Override
    protected Path resolveLink(Path f) throws IOException {
        return getLinkTarget(f);
    }

    @Override
    public FileChecksum getFileChecksum(Path f, long length) throws IOException {
        judgeActualFSInitialized();
        return this.actualImplFS.getFileChecksum(f, length);
    }

    @Override
    public void setVerifyChecksum(boolean verifyChecksum) {
        judgeActualFSInitialized();
        this.actualImplFS.setVerifyChecksum(verifyChecksum);
    }

    @Override
    public void setWriteChecksum(boolean writeChecksum) {
        judgeActualFSInitialized();
        this.actualImplFS.setWriteChecksum(writeChecksum);
    }

    @Override
    public FsStatus getStatus(Path p) throws IOException {
        judgeActualFSInitialized();
        return this.actualImplFS.getStatus(p);
    }

    @Override
    public void setPermission(Path p, FsPermission permission) throws IOException {
        judgeActualFSInitialized();
        this.actualImplFS.setPermission(p, permission);
    }

    @Override
    public void setOwner(Path p, String username, String groupname) throws IOException {
        judgeActualFSInitialized();
        this.actualImplFS.setOwner(p, username, groupname);
    }

    @Override
    public void setTimes(Path p, long mtime, long atime) throws IOException {
        judgeActualFSInitialized();
        this.actualImplFS.setTimes(p, mtime, atime);
    }

    @Override
    public Path createSnapshot(Path path, String snapshotName) throws IOException {
        judgeActualFSInitialized();
        return this.actualImplFS.createSnapshot(path, snapshotName);
    }

    @Override
    public void renameSnapshot(Path path, String snapshotOldName, String snapshotNewName) throws IOException {
        judgeActualFSInitialized();
        this.actualImplFS.renameSnapshot(path, snapshotOldName, snapshotNewName);
    }

    @Override
    public void deleteSnapshot(Path path, String snapshotName) throws IOException {
        judgeActualFSInitialized();
        this.actualImplFS.deleteSnapshot(path, snapshotName);
    }

    @Override
    public void modifyAclEntries(Path path, List<AclEntry> aclSpec) throws IOException {
        judgeActualFSInitialized();
        this.actualImplFS.modifyAclEntries(path, aclSpec);
    }

    @Override
    public void removeAclEntries(Path path, List<AclEntry> aclSpec) throws IOException {
        judgeActualFSInitialized();
        this.actualImplFS.removeAclEntries(path, aclSpec);
    }

    @Override
    public void removeDefaultAcl(Path path) throws IOException {
        judgeActualFSInitialized();
        this.actualImplFS.removeDefaultAcl(path);
    }

    @Override
    public void removeAcl(Path path) throws IOException {
        judgeActualFSInitialized();
        this.actualImplFS.removeAcl(path);
    }

    @Override
    public void setAcl(Path path, List<AclEntry> aclSpec) throws IOException {
        judgeActualFSInitialized();
        this.actualImplFS.setAcl(path, aclSpec);
    }

    @Override
    public AclStatus getAclStatus(Path path) throws IOException {
        judgeActualFSInitialized();
        return this.actualImplFS.getAclStatus(path);
    }

    @Override
    public void setXAttr(Path path, String name, byte[] value, EnumSet<XAttrSetFlag> flag) throws IOException {
        judgeActualFSInitialized();
        this.actualImplFS.setXAttr(path, name, value, flag);
    }

    @Override
    public byte[] getXAttr(Path path, String name) throws IOException {
        judgeActualFSInitialized();
        return this.actualImplFS.getXAttr(path, name);
    }

    @Override
    public Map<String, byte[]> getXAttrs(Path path) throws IOException {
        judgeActualFSInitialized();
        return this.actualImplFS.getXAttrs(path);
    }

    @Override
    public Map<String, byte[]> getXAttrs(Path path, List<String> names) throws IOException {
        judgeActualFSInitialized();
        return this.actualImplFS.getXAttrs(path, names);
    }

    @Override
    public List<String> listXAttrs(Path path) throws IOException {
        judgeActualFSInitialized();
        return this.actualImplFS.listXAttrs(path);
    }

    @Override
    public void removeXAttr(Path path, String name) throws IOException {
        judgeActualFSInitialized();
        this.actualImplFS.removeXAttr(path, name);
    }

    @Override
    public Token<?> getDelegationToken(String renewer) throws IOException {
        judgeActualFSInitialized();
        return this.actualImplFS.getDelegationToken(renewer);
    }

    @Override
    public String getCanonicalServiceName() {
        if (this.actualImplFS == null) {
            return null;
        } else {
            return this.actualImplFS.getCanonicalServiceName();
        }
    }

    @Override
    public ContentSummary getContentSummary(Path f) throws IOException {
        judgeActualFSInitialized();
        return this.actualImplFS.getContentSummary(f);
    }

    @Override
    public void releaseFileLock(Path p) throws IOException {
        if (this.actualImplFS == null) {
            throw new IOException("please init the fileSystem first!");
        }
        this.actualImplFS.releaseFileLock(p);
    }


    @Override
    public void close() throws IOException {
        judgeActualFSInitialized();
        super.close();
        this.actualImplFS.close();
    }
}
