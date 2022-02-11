package com.qcloud.chdfs.fs;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.util.VersionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.URLConnection;
import java.net.URLEncoder;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;

class CHDFSHadoopFileSystemJarLoader {

    private static final Logger log = LoggerFactory.getLogger(CHDFSHadoopFileSystemJarLoader.class);
    private static AlreadyLoadedFileSystemInfo alreadyLoadedFileSystemInfo;
    private String versionId;
    private String jarPath;
    private String jarMd5;
    private FileSystemWithLockCleaner actualFileSystem;

    CHDFSHadoopFileSystemJarLoader() {
    }

    synchronized void init(String mountPointAddr, long appid, int jarPluginServerPort, String tmpDirPath,
            boolean jarPluginServerHttps) throws IOException {
        if (this.actualFileSystem == null) {
            long queryStartMs = System.currentTimeMillis();
            queryJarPluginInfo(mountPointAddr, appid, jarPluginServerPort, jarPluginServerHttps);
            log.debug("query jar plugin info usedMs: {}", System.currentTimeMillis() - queryStartMs);
            this.actualFileSystem = getAlreadyLoadedClassInfo(this.getClass().getClassLoader(), this.jarPath,
                    this.versionId, this.jarMd5, tmpDirPath);
            if (this.actualFileSystem == null) {
                String errMsg = "CHDFSHadoopFileSystemJarLoader getAlreadyLoadedClassInfo return null";
                throw new IOException(errMsg);
            }
        }
    }

    private void parseJarPluginInfoResp(String respStr) throws IOException {
        JsonObject respJson = new JsonParser().parse(respStr).getAsJsonObject();
        if (!respJson.has("Response")) {
            String errMsg = String.format("resp json miss element Response, resp: %s", respStr);
            log.error(errMsg);
            throw new IOException(errMsg);
        }

        if (!respJson.get("Response").getAsJsonObject().has("HadoopPluginJar")) {
            String errMsg = String.format("resp json miss element Response.HadoopPluginJar, resp: %s", respStr);
            log.error(errMsg);
            throw new IOException(errMsg);
        }
        JsonObject jarInfoJson = respJson.get("Response").getAsJsonObject().get("HadoopPluginJar").getAsJsonObject();
        if (!jarInfoJson.has("VersionId")) {
            String errMsg = String.format("resp miss config Response.HadoopPluginJar.VersionId, resp: %s", respStr);
            log.error(errMsg);
            throw new IOException(errMsg);
        } else {
            this.versionId = jarInfoJson.get("VersionId").getAsString();
        }

        if (!jarInfoJson.has("JarPath")) {
            String errMsg = String.format("resp miss config Response.HadoopPluginJar.JarPath, resp: %s", respStr);
            log.error(errMsg);
            throw new IOException(errMsg);
        } else {
            this.jarPath = jarInfoJson.get("JarPath").getAsString();
        }

        if (!jarInfoJson.has("JarMd5")) {
            String errMsg = String.format("resp miss config Response.HadoopPluginJar.JarMd5, resp: %s", respStr);
            log.error(errMsg);
            throw new IOException(errMsg);
        } else {
            this.jarMd5 = jarInfoJson.get("JarMd5").getAsString();
        }
    }

    private void queryJarPluginInfo(String mountPointAddr, long appid, int jarPluginServerPort,
            boolean jarPluginServerHttpsFlag) throws IOException {
        String hadoopVersion = VersionInfo.getVersion();
        if (hadoopVersion == null) {
            hadoopVersion = "unknown";
        }

        URL queryJarUrl = null;
        String queryJarUrlStr = "";
        try {
            queryJarUrlStr = String.format("%s://%s:%d/chdfs-hadoop-plugin?appid=%d&hadoop_version=%s",
                    jarPluginServerHttpsFlag ? "https" : "http", mountPointAddr, jarPluginServerPort, appid,
                    URLEncoder.encode(hadoopVersion.trim(), "UTF-8"));
            queryJarUrl = new URL(queryJarUrlStr);
        } catch (MalformedURLException | UnsupportedEncodingException e) {
            String errMsg = String.format("invalid url %s", queryJarUrlStr);
            log.error(errMsg, e);
            throw new IOException(errMsg, e);
        }

        long startTimeNs = System.nanoTime();
        HttpURLConnection conn = null;
        BufferedInputStream bis = null;
        ByteArrayOutputStream bos = null;
        try {
            conn = (HttpURLConnection) queryJarUrl.openConnection();
            conn.setRequestProperty("Connection", "Keep-Alive");
            conn.setReadTimeout(120000);
            conn.connect();

            bis = new BufferedInputStream(conn.getInputStream());
            bos = new ByteArrayOutputStream();
            byte[] buf = new byte[4096];
            int readLen = 0;
            while ((readLen = bis.read(buf)) != -1) {
                bos.write(buf, 0, readLen);
            }
            String respStr = bos.toString();
            parseJarPluginInfoResp(respStr);
        } catch (IOException e) {
            String errMsg = "queryJarPluginInfo occur an io exception";
            log.error(errMsg, e);
            throw new IOException(errMsg, e);
        } finally {
            if (bis != null) {
                IOUtils.closeStream(bis);
            }
            if (bos != null) {
                IOUtils.closeStream(bos);
            }
            if (conn != null) {
                conn.disconnect();
            }
        }
        log.debug("query jarPluginInfo, usedTimeMs: {}", (System.nanoTime() - startTimeNs) * 1.0 / 1000000);
    }

    private static synchronized FileSystemWithLockCleaner getAlreadyLoadedClassInfo(ClassLoader currentClassLoader,
            String jarPath, String versionId, String jarMd5, String tmpDirPath) throws IOException {
        if (alreadyLoadedFileSystemInfo != null && alreadyLoadedFileSystemInfo.jarPath.equals(jarPath)
                && alreadyLoadedFileSystemInfo.versionId.equals(versionId) && alreadyLoadedFileSystemInfo.jarMd5.equals(
                jarMd5)) {
            try {
                return (FileSystemWithLockCleaner) alreadyLoadedFileSystemInfo.chdfsFSClass.newInstance();
            } catch (InstantiationException | IllegalAccessException e) {
                String errMsg = String.format("load chdfs class failed, className: %s",
                        alreadyLoadedFileSystemInfo.chdfsFSClass.getName());
                log.error(errMsg, e);
                throw new IOException(errMsg, e);
            }
        }

        File jarFile = downloadJarPath(jarPath, versionId, jarMd5, tmpDirPath);
        URL jarUrl = null;
        try {
            jarUrl = jarFile.toURI().toURL();
        } catch (MalformedURLException e) {
            String errMsg = String.format("get jar file url failed, jarPath: %s", jarFile.getAbsolutePath());
            log.error(errMsg, e);
            throw new IOException(errMsg, e);
        }
        URLClassLoader chdfsJarClassLoader = new URLClassLoader(new URL[]{jarUrl}, currentClassLoader);
        final String className = String.format("chdfs.%s.com.qcloud.chdfs.fs.CHDFSHadoopFileSystem", versionId);
        try {
            Class chdfsFSClass = chdfsJarClassLoader.loadClass(className);
            FileSystemWithLockCleaner actualFileSystem = (FileSystemWithLockCleaner) chdfsFSClass.newInstance();
            alreadyLoadedFileSystemInfo = new AlreadyLoadedFileSystemInfo(versionId, jarPath, jarMd5, chdfsFSClass);
            return actualFileSystem;
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
            String errMsg = String.format("load class failed, className: %s", className);
            log.error(errMsg, e);
            throw new IOException(errMsg, e);
        }
    }

    private static File downloadJarPath(String jarPath, String versionId, String jarMd5, String tmpDirPath)
            throws IOException {
        File localCacheJarFile = new File(String.format("%s/chdfs_hadoop_plugin-%s-shaded.jar", tmpDirPath, versionId));
        File localCacheJarLockFile = new File(
                String.format("%s/chdfs_hadoop_plugin-%s-shaded.jar.LOCK", tmpDirPath, versionId));
        if (localCacheJarFile.exists()) {
            String md5Hex = getFileHexMd5(localCacheJarFile);
            if (md5Hex.equalsIgnoreCase(jarMd5)) {
                return localCacheJarFile;
            }
        }

        FileOutputStream fileLockOutPut = null;
        FileLock fileLock = null;
        try {
            fileLockOutPut = new FileOutputStream(localCacheJarLockFile);
        } catch (IOException e) {
            String errMsg = String.format("download jar failed, open lock file failed, lockPath: %s",
                    localCacheJarLockFile.getAbsolutePath());
            log.error(errMsg, e);
            throw new IOException(errMsg, e);
        }

        while (true) {
            try {
                fileLock = fileLockOutPut.getChannel().lock();
                break;
            } catch (OverlappingFileLockException ofle) {
                try {
                    Thread.sleep(10L);
                } catch (InterruptedException e) {
                    try {
                        fileLockOutPut.close();
                    } catch (IOException ignore) {
                    }
                    String errMsg = String.format("download jar failed, lock file failed, lockPath: %s",
                            localCacheJarLockFile.getAbsolutePath());
                    log.error(errMsg, e);
                    throw new IOException(errMsg, e);
                }
            } catch (IOException e) {
                try {
                    fileLockOutPut.close();
                } catch (IOException ignore) {
                }
                String errMsg = String.format("download jar failed, lock file failed, lockPath: %s",
                        localCacheJarLockFile.getAbsolutePath());
                log.error(errMsg, e);
                throw new IOException(errMsg, e);
            }
        }

        BufferedInputStream bis = null;
        BufferedOutputStream fos = null;
        try {

            // judge again may be other process has download the jar
            if (localCacheJarFile.exists()) {
                String md5Hex = getFileHexMd5(localCacheJarFile);
                if (md5Hex.equalsIgnoreCase(jarMd5)) {
                    return localCacheJarFile;
                }
            }
            URL downloadJarUrl = null;
            try {
                downloadJarUrl = new URL(jarPath);
            } catch (MalformedURLException e) {
                String errMsg = String.format("invalid download jar url %s", jarPath);
                log.error(errMsg, e);
                throw new IOException(errMsg, e);
            }

            try {
                URLConnection conn = downloadJarUrl.openConnection();
                conn.connect();
                bis = new BufferedInputStream(conn.getInputStream());
                fos = new BufferedOutputStream(new FileOutputStream(localCacheJarFile));
                IOUtils.copyBytes(bis, fos, 4096, true);

                // set jar and lock file permission 777
                localCacheJarFile.setReadable(true, false);
                localCacheJarFile.setWritable(true, false);
                localCacheJarFile.setExecutable(true, false);

                localCacheJarLockFile.setReadable(true, false);
                localCacheJarLockFile.setWritable(true, false);
                localCacheJarLockFile.setExecutable(true, false);

                bis = null;
                fos = null;

                fileLock.release();
                fileLock = null;

                fileLockOutPut.close();
                fileLockOutPut = null;
            } catch (IOException e) {
                String errMsg = String.format("download jar failed, localJarPath: %s",
                        localCacheJarFile.getAbsolutePath());
                log.error(errMsg, e);
                throw new IOException(errMsg);
            }

            String md5Hex = getFileHexMd5(localCacheJarFile);
            if (!md5Hex.equalsIgnoreCase(jarMd5)) {
                String errMsg = String.format("download jar md5 check failed, local jar md5: %s, query jar md5: %s",
                        md5Hex, jarMd5);
                log.error(errMsg);
                throw new IOException(errMsg);
            }
            return localCacheJarFile;
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

    private static String getFileHexMd5(File inFile) throws IOException {
        FileInputStream in = null;
        try {
            in = new FileInputStream(inFile);
            byte[] md5Byte = MD5Hash.digest(in).getDigest();
            return Hex.encodeHexString(md5Byte);
        } catch (IOException e) {
            String errMsg = String.format("getFileHexMd5 failed, inFile path: %s", inFile.getAbsolutePath());
            log.error(errMsg, e);
            throw new IOException(errMsg, e);
        } finally {
            IOUtils.closeStream(in);
        }
    }

    FileSystemWithLockCleaner getActualFileSystem() {
        return actualFileSystem;
    }
}
