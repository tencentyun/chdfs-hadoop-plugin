package com.qcloud.chdfs.fs;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.util.VersionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.*;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;

class CHDFSHadoopFileSystemJarLoader {

    private static final Logger log = LoggerFactory.getLogger(CHDFSHadoopFileSystemJarLoader.class);
    private String versionId;
    private String jarPath;
    private String jarMd5;

    private FileSystem actualFileSystem;
    private static AlreadyLoadedFileSystemInfo alreadyLoadedFileSystemInfo;

    private String chdfsDataTransferEndpointSuffix;

    CHDFSHadoopFileSystemJarLoader() {
    }

    public CHDFSHadoopFileSystemJarLoader(String jarPath) {
        this.jarPath = jarPath;
    }

    public String getJarPath() {
        return jarPath;
    }

    void modifyJarPathAccordToEndpointSuffix() {
        if (jarPath == null || chdfsDataTransferEndpointSuffix == null || chdfsDataTransferEndpointSuffix.isEmpty()) {
            return;
        }
        if (!chdfsDataTransferEndpointSuffix.startsWith(".")) {
            chdfsDataTransferEndpointSuffix = "." + chdfsDataTransferEndpointSuffix;
        }
        jarPath = jarPath.substring(0, jarPath.indexOf(".")) + chdfsDataTransferEndpointSuffix;
        if (jarPath.startsWith("https://")) {
            jarPath = jarPath.replace("https://", "http://");
        }
    }

    private boolean queryJarPluginInfo(String mountPointAddr, long appid, int jarPluginServerPort, boolean jarPluginServerHttpsFlag) {
        String hadoopVersion = VersionInfo.getVersion();
        if (hadoopVersion == null) {
            hadoopVersion = "unknown";
        }
        URL queryJarUrl = null;
        try {
            queryJarUrl = new URL(
                    String.format("%s://%s:%d/chdfs-hadoop-plugin?appid=%d&hadoop_version=%s",
                            jarPluginServerHttpsFlag ? "https" : "http", mountPointAddr, jarPluginServerPort, appid, URLEncoder.encode(hadoopVersion.trim(), "UTF-8")));
        } catch (MalformedURLException | UnsupportedEncodingException e) {
            log.error("invalid url", e);
            return false;
        }

        long startTimeNs = System.nanoTime();
        try {
            HttpURLConnection conn = (HttpURLConnection) queryJarUrl.openConnection();
            conn.setRequestProperty("Connection", "Keep-Alive");
            conn.connect();

            BufferedInputStream bis = new BufferedInputStream(conn.getInputStream());
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            byte[] buf = new byte[4096];
            int readLen = 0;
            while ((readLen = bis.read(buf)) != -1) {
                bos.write(buf, 0, readLen);
            }

            String respStr = bos.toString();
            JsonObject respJson = new JsonParser().parse(respStr).getAsJsonObject();

            if (!respJson.has("Response")) {
                log.error("resp json miss element Response, resp: {}", respStr);
                return false;
            }

            if (!respJson.get("Response").getAsJsonObject()
                    .has("HadoopPluginJar")) {
                log.error(
                        "resp json miss element Response.HadoopPluginJar, resp: {}",
                        respStr);
                return false;
            }

            JsonObject jarInfoJson = respJson.get("Response").getAsJsonObject()
                    .get("HadoopPluginJar").getAsJsonObject();
            if (!jarInfoJson.has("VersionId")) {
                log.error(
                        "resp miss config Response.HadoopPluginJar.VersionId, resp: {}",
                        respStr);
                return false;
            } else {
                this.versionId = jarInfoJson.get("VersionId").getAsString();
            }

            if (!jarInfoJson.has("JarPath")) {
                log.error(
                        "resp miss config Response.HadoopPluginJar.JarPath, resp: {}",
                        respStr);
                return false;
            } else {
                this.jarPath = jarInfoJson.get("JarPath").getAsString();
            }

            if (!jarInfoJson.has("JarMd5")) {
                log.error(
                        "resp miss config Response.HadoopPluginJar.JarMd5, resp: {}",
                        respStr);
                return false;
            } else {
                this.jarMd5 = jarInfoJson.get("JarMd5").getAsString();
            }
            log.debug("query jarPluginInfo, usedTimeMs: {}", (System.nanoTime() - startTimeNs) * 1.0 / 1000000);
        } catch (IOException e) {
            log.error("queryJarPluginInfo occur an io exception", e);
            return false;
        }
        return true;
    }


    synchronized boolean init(String mountPointAddr, long appid, int jarPluginServerPort, String tmpDirPath, boolean jarPluginServerHttps) {
        if (this.actualFileSystem == null) {
            long queryStartMs = System.currentTimeMillis();
            if (!queryJarPluginInfo(mountPointAddr, appid, jarPluginServerPort, jarPluginServerHttps)) {
                return false;
            }
            log.debug("query jar plugin info usedMs: {}", System.currentTimeMillis() - queryStartMs);
            this.actualFileSystem = getAlreadyLoadedClassInfo(this.getClass().getClassLoader(), this.jarPath, this.versionId, this.jarMd5, tmpDirPath);
            if (this.actualFileSystem == null) {
                return false;
            }
        }
        return true;
    }

    FileSystem getActualFileSystem() {
        return actualFileSystem;
    }

    public void setChdfsDataTransferEndpointSuffix(String chdfsDataTransferEndpointSuffix) {
        this.chdfsDataTransferEndpointSuffix = chdfsDataTransferEndpointSuffix;
    }

    private static synchronized FileSystem getAlreadyLoadedClassInfo(ClassLoader currentClassLoader, String jarPath, String versionId, String jarMd5, String tmpDirPath) {
        if (alreadyLoadedFileSystemInfo != null
                && alreadyLoadedFileSystemInfo.jarPath.equals(jarPath)
                && alreadyLoadedFileSystemInfo.versionId.equals(versionId)
                && alreadyLoadedFileSystemInfo.jarMd5.equals(jarMd5)
        ) {
            return alreadyLoadedFileSystemInfo.actualFileSystem;
        }

        File jarFile = downloadJarPath(jarPath, versionId, jarMd5, tmpDirPath);
        if (jarFile == null) {
            log.error("download jar file failed");
            return null;
        }
        URL jarUrl = null;
        try {
            jarUrl = jarFile.toURI().toURL();
        } catch (MalformedURLException e) {
            log.error("get jar url failed.", e);
            return null;
        }
        URLClassLoader chdfsJarClassLoader = new URLClassLoader(new URL[]{jarUrl}, currentClassLoader);
        final String className = String.format(
                "chdfs.%s.com.qcloud.chdfs.fs.CHDFSHadoopFileSystem", versionId);
        try {
            Class chdfsFSClass = chdfsJarClassLoader.loadClass(className);
            FileSystem actualFileSystem = (FileSystem) chdfsFSClass.newInstance();
            alreadyLoadedFileSystemInfo = new AlreadyLoadedFileSystemInfo(versionId, jarPath, jarMd5, actualFileSystem);
            return actualFileSystem;
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
            log.error("load class failed", e);
            return null;
        }
    }

    private static String getFileHexMd5(File inFile) {
        FileInputStream in = null;
        try {
            in = new FileInputStream(inFile);
            byte[] md5Byte = MD5Hash.digest(in).getDigest();
            return Hex.encodeHexString(md5Byte);
        } catch (IOException e) {
            log.error(String.format("getFileHexMd5 failed, inFile path: %s", inFile.getAbsolutePath()), e);
            return null;
        } finally {
            IOUtils.closeStream(in);
        }
    }

    private static File downloadJarPath(String jarPath, String versionId, String jarMd5, String tmpDirPath) {
        File localCacheJarFile = new File(String.format("%s/chdfs_hadoop_plugin-%s-shaded.jar", tmpDirPath, versionId));
        File localCacheJarLockFile = new File(String.format("%s/chdfs_hadoop_plugin-%s-shaded.jar.LOCK", tmpDirPath, versionId));
        if (localCacheJarFile.exists()) {
            String md5Hex = getFileHexMd5(localCacheJarFile);
            if (md5Hex != null && md5Hex.equalsIgnoreCase(jarMd5)) {
                return localCacheJarFile;
            }
        }

        FileOutputStream fileLockOutPut = null;
        FileLock fileLock = null;
        try {
            fileLockOutPut = new FileOutputStream(localCacheJarLockFile);
        } catch (IOException e) {
            log.error(String.format("download jar failed, open lock file failed, lockPath: %s", localCacheJarLockFile.getAbsolutePath()), e);
            return null;
        }
        while (true) {
            try {
                fileLock = fileLockOutPut.getChannel().lock();
                break;
            } catch (OverlappingFileLockException ofle) {
                try {
                    Thread.sleep(10L);
                } catch (InterruptedException e) {
                    log.error(String.format("download jar failed, lock file failed, lockPath: %s", localCacheJarLockFile.getAbsolutePath()), e);
                    try {
                        fileLockOutPut.close();
                    } catch (IOException ignore) {
                    }
                    return null;
                }
            } catch (IOException e) {
                log.error(String.format("download jar failed, lock file failed, lockPath: %s", localCacheJarLockFile.getAbsolutePath()), e);
                try {
                    fileLockOutPut.close();
                } catch (IOException ignore) {
                }
                return null;
            }
        }

        BufferedInputStream bis = null;
        BufferedOutputStream fos = null;
        try {

            // judge again may be other process has download the jar
            if (localCacheJarFile.exists()) {
                String md5Hex = getFileHexMd5(localCacheJarFile);
                if (md5Hex != null && md5Hex.equalsIgnoreCase(jarMd5)) {
                    return localCacheJarFile;
                }
            }
            URL downloadJarUrl = null;
            try {
                downloadJarUrl = new URL(jarPath);
            } catch (MalformedURLException e) {
                log.error("invalid url", e);
                return null;
            }

            try {
                URLConnection conn = downloadJarUrl.openConnection();
                conn.connect();
                bis = new BufferedInputStream(conn.getInputStream());
                fos = new BufferedOutputStream(new FileOutputStream(localCacheJarFile));
                IOUtils.copyBytes(bis, fos, 4096, true);
                bis = null;
                fos = null;
                fileLock.release();
                fileLock = null;

                fileLockOutPut.close();
                fileLockOutPut = null;

                String md5Hex = getFileHexMd5(localCacheJarFile);
                if (md5Hex == null) {
                    log.error("get jar file md5 failed, localJarPath: {}", localCacheJarFile.getAbsolutePath());
                    return null;
                }
                if (!md5Hex.equalsIgnoreCase(jarMd5)) {
                    log.error("download jar md5 check failed, local jar md5: {}, query jar md5: {}", md5Hex, jarMd5);
                    return null;
                }
                return localCacheJarFile;
            } catch (IOException e) {
                log.error(String.format("download jar failed, localJarPath: %s", localCacheJarFile.getAbsolutePath()), e);
                return null;
            }
        } finally {
            if (fileLock != null) {
                try {
                    fileLock.release();
                } catch (IOException ignored) {
                }
            }

            if (fileLockOutPut != null) {
                try {
                    fileLockOutPut.close();
                } catch (IOException ignored) {
                }
            }

            if (bis != null) {
                try {
                    bis.close();
                } catch (IOException ignored) {
                }
            }

            if (fos != null) {
                try {
                    fos.close();
                } catch (IOException ignored) {
                }
            }
        }
    }

}
