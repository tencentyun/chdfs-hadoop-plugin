package com.qcloud.chdfs.fs;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.URLConnection;
import java.net.URLEncoder;
import java.nio.channels.FileLock;

import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.util.VersionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class CHDFSHadoopFileSystemJarLoader {

    static final CHDFSHadoopFileSystemJarLoader INSTANCE = new CHDFSHadoopFileSystemJarLoader();
    private static final Logger log = LoggerFactory.getLogger(CHDFSHadoopFileSystemJarLoader.class);
    private String versionId;
    private String jarPath;
    private String jarMd5;

    private FileSystem actualFileSystem;

    private CHDFSHadoopFileSystemJarLoader() {

    }

    private boolean queryJarPluginInfo(String mountPointAddr, long appid, int jarPluginServerPort) {
        String hadoopVersion = VersionInfo.getVersion();
        if (hadoopVersion == null) {
            hadoopVersion = "unknown";
        }
        URL queryJarUrl = null;
        try {
            queryJarUrl = new URL(
                    String.format("http://%s:%d/chdfs-hadoop-plugin?appid=%d&hadoop_version=%s",
                            mountPointAddr, jarPluginServerPort, appid, URLEncoder.encode(hadoopVersion.trim(), "UTF-8")));
        } catch (MalformedURLException | UnsupportedEncodingException e) {
            log.error("invalid url", e);
            return false;
        }

        try {
            URLConnection conn = queryJarUrl.openConnection();
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

            return true;
        } catch (IOException e) {
            log.error("queryJarPluginInfo occur an io exception", e);
            return false;
        }
    }


    private String getFileHexMd5(File inFile) {
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

    private File downloadJarPath(String tmpDirPath) {
        File localCacheJarFile = new File(String.format("%s/chdfs_hadoop_plugin-%s-shaded.jar", tmpDirPath, versionId));
        if (localCacheJarFile.exists()) {
            String md5Hex = getFileHexMd5(localCacheJarFile);
            if (md5Hex != null && md5Hex.equalsIgnoreCase(jarMd5)) {
                return localCacheJarFile;
            }
        }

        FileOutputStream fos = null;
        FileLock fileLock = null;
        try {
            fos = new FileOutputStream(localCacheJarFile);
            fileLock = fos.getChannel().lock();
        } catch (IOException e) {
            log.error(String.format("download jar failed, localJarPath: %s", localCacheJarFile.getAbsolutePath()), e);
            return null;
        }

        BufferedInputStream bis = null;
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
                IOUtils.copyBytes(bis, fos, 4096, true);
                bis.close();
                bis = null;

                fos.flush();
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
            try {
                fileLock.release();
            } catch (IOException ignored) {
            }

            try {
                fos.close();
            } catch (IOException ignored) {
            }

            if (bis != null) {
                try {
                    bis.close();
                } catch (IOException ignored) {
                }
            }
        }
    }

    synchronized boolean init(String mountPointAddr, long appid, int jarPluginServerPort, String tmpDirPath) {
        if (actualFileSystem == null) {
            if (!queryJarPluginInfo(mountPointAddr, appid, jarPluginServerPort)) {
                return false;
            }

            File jarFile = downloadJarPath(tmpDirPath);
            if (jarFile == null) {
                log.error("download jar file failed");
                return false;
            }
            URL jarUrl = null;
            try {
                jarUrl = jarFile.toURI().toURL();
            } catch (MalformedURLException e) {
                log.error("get jar url failed.", e);
                return false;
            }
            URLClassLoader chdfsJarClassLoader = new URLClassLoader(new URL[]{jarUrl},
                    Thread.currentThread().getContextClassLoader());
            final String className = String.format(
                    "chdfs.%s.com.qcloud.chdfs.fs.CHDFSHadoopFileSystem", versionId);
            try {
                Class chdfsFSClass = chdfsJarClassLoader.loadClass(className);
                this.actualFileSystem = (FileSystem) chdfsFSClass.newInstance();
            } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
                log.error("load class failed", e);
                return false;
            }
        }
        return true;
    }

    FileSystem getActualFileSystem() {
        return actualFileSystem;
    }
}
