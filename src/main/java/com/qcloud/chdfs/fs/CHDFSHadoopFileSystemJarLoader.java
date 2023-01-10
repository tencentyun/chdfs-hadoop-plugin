package com.qcloud.chdfs.fs;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.util.VersionInfo;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
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
import java.net.URLEncoder;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;

class CHDFSHadoopFileSystemJarLoader {

    private static final Logger log = LoggerFactory.getLogger(CHDFSHadoopFileSystemJarLoader.class);
    private static AlreadyLoadedFileSystemInfo alreadyLoadedFileSystemInfo;
    private String versionId;
    private String jarPath;

    private String jarHost;
    private String jarMd5;
    private FileSystemWithLockCleaner actualFileSystem;

    CHDFSHadoopFileSystemJarLoader() {
    }

    synchronized void init(String mountPointAddr, long appid, int jarPluginServerPort, String tmpDirPath,
                           boolean jarPluginServerHttps, String cosEndPointSuffix, boolean distinguishHost,
                           String networkVersionId, boolean useL5, String metaL5Address, String cosL5Address) throws IOException {
        if (this.actualFileSystem == null) {
            long queryStartMs = System.currentTimeMillis();
            queryJarPluginInfo(mountPointAddr, appid, jarPluginServerPort, jarPluginServerHttps, cosEndPointSuffix,
                    useL5, metaL5Address);
            log.debug("query jar plugin info usedMs: {}", System.currentTimeMillis() - queryStartMs);
            this.actualFileSystem = getAlreadyLoadedClassInfo(this.getClass().getClassLoader(), this.jarPath,
                    this.versionId, this.jarMd5, tmpDirPath, this.jarHost, distinguishHost, networkVersionId, useL5, cosL5Address);
            if (this.actualFileSystem == null) {
                String errMsg = "CHDFSHadoopFileSystemJarLoader getAlreadyLoadedClassInfo return null";
                throw new IOException(errMsg);
            }
        }
    }

    private void parseJarPluginInfoResp(String respStr, String cosEndPointSuffix) throws IOException {
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
            this.jarHost = new URL(jarInfoJson.get("JarPath").getAsString()).getAuthority();
            if (cosEndPointSuffix != null) {
                String jarPath = jarInfoJson.get("JarPath").getAsString();
                int dotIndex = jarPath.indexOf('.');
                if (dotIndex == -1) {
                    String errMsg = String.format("invalid jar path : %s", jarPath);
                    log.error(errMsg);
                    throw new IOException(errMsg);
                }

                int slashIndex = jarPath.indexOf('/', dotIndex);
                if (slashIndex == -1) {
                    String errMsg = String.format("invalid jar path : %s", jarPath);
                    log.error(errMsg);
                    throw new IOException(errMsg);
                }
                this.jarPath = jarPath.substring(0, dotIndex + 1) + cosEndPointSuffix + jarPath.substring(slashIndex);
            } else {
                this.jarPath = jarInfoJson.get("JarPath").getAsString();
            }
        }

        if (!jarInfoJson.has("JarMd5")) {
            String errMsg = String.format("resp miss config Response.HadoopPluginJar.JarMd5, resp: %s", respStr);
            log.error(errMsg);
            throw new IOException(errMsg);
        } else {
            this.jarMd5 = jarInfoJson.get("JarMd5").getAsString();
        }
    }

    private static String getL5Endpoint(String l5Address) throws IOException {
        try {
            String[] address = l5Address.split(ConfigKey.L5Separator);
            L5EndpointResolver l5EndpointResolver = new L5EndpointResolver(Integer.parseInt(address[0]),
                    Integer.parseInt(address[1]));
            String l5Endpoint = l5EndpointResolver.resolveGeneralApiEndpoint();
            if (StringUtils.isEmpty(l5Endpoint)) {
                throw new IOException(
                        String.format("can not get ip and port from l5 address %s", l5Address));
            }
            return l5Endpoint;
        } catch (NumberFormatException e) {
            throw new IOException(
                    String.format("l5 address: %s is invalid", l5Address));
        }
    }

    private void queryJarPluginInfo(String mountPointAddr, long appid, int jarPluginServerPort,
                                    boolean jarPluginServerHttpsFlag, String cosEndPointSuffix,
                                    boolean useL5, String metaL5Address) throws IOException {
        String hadoopVersion = VersionInfo.getVersion();
        if (hadoopVersion == null) {
            hadoopVersion = "unknown";
        }

        URL queryJarUrl = null;
        String queryJarUrlStr = "";
        try {
            if (!useL5) {
                queryJarUrlStr = String.format("%s://%s:%d/chdfs-hadoop-plugin?appid=%d&hadoop_version=%s",
                        jarPluginServerHttpsFlag ? "https" : "http", mountPointAddr, jarPluginServerPort, appid,
                        URLEncoder.encode(hadoopVersion.trim(), "UTF-8"));
            } else {
                String l5Endpoint = getL5Endpoint(metaL5Address);
                log.debug("get meta address success: {}", l5Endpoint);
                queryJarUrlStr = String.format("%s://%s/chdfs-hadoop-plugin?appid=%d&hadoop_version=%s",
                        "http", l5Endpoint, appid,
                        URLEncoder.encode(hadoopVersion.trim(), "UTF-8"));
            }
            queryJarUrl = new URL(queryJarUrlStr);

        } catch (MalformedURLException | UnsupportedEncodingException | NumberFormatException e) {
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
            parseJarPluginInfoResp(respStr, cosEndPointSuffix);
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
                                                                                    String jarPath, String versionId,
                                                                                    String jarMd5, String tmpDirPath,
                                                                                    String jarHost,
                                                                                    boolean distinguishHost,
                                                                                    String networkVersionId, boolean useL5, String cosL5Address) throws IOException {
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

        File jarFile = downloadJarPath(jarPath, versionId, jarMd5, tmpDirPath, jarHost, distinguishHost,
                networkVersionId, useL5, cosL5Address);
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

    private static File downloadJarPath(String jarPath, String versionId, String jarMd5, String tmpDirPath,
                                        String jarHost, boolean distinguishHost, String networkVersionId, boolean useL5, String cosL5Address)
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
            if (useL5) {
                String l5Endpoint = getL5Endpoint(cosL5Address);
                replaceCosHost(jarPath, l5Endpoint);
            }
            CloseableHttpClient httpclient = null;
            CloseableHttpResponse response = null;
            HttpGet httpGet = null;
            try {
                httpclient = HttpClients.createDefault();
                httpGet = new HttpGet(jarPath);
                httpGet.setHeader("User-Agent", String.format("chdfs_hadoop-plugin_network-%s", networkVersionId));
                if (distinguishHost) {
                    httpGet.addHeader("Host", jarHost);
                    log.debug("host: {} already set", jarHost);
                }

                // execute request
                response = httpclient.execute(httpGet);
                // judge status code == 200
                if (response.getStatusLine().getStatusCode() == 200) {
                    // get content
                    bis = new BufferedInputStream(response.getEntity().getContent());
                    fos = new BufferedOutputStream(new FileOutputStream(localCacheJarFile));
                    IOUtils.copyBytes(bis, fos, 4096, true);

                    // set jar and lock file permission 777
                    localCacheJarFile.setReadable(true, false);
                    localCacheJarFile.setWritable(true, false);
                    localCacheJarFile.setExecutable(true, false);

                    localCacheJarLockFile.setReadable(true, false);
                    localCacheJarLockFile.setWritable(true, false);
                    localCacheJarLockFile.setExecutable(true, false);
                }

                httpGet.releaseConnection();
            } catch (IOException e) {
                httpGet.abort();
                String errMsg = String.format("download jar failed, localJarPath: %s",
                        localCacheJarFile.getAbsolutePath());
                log.error(errMsg, e);
                throw new IOException(errMsg);
            } finally {
                if (response != null) {
                    try {
                        response.close();
                    } catch (IOException ignored) {
                    }
                }
                if (httpclient != null) {
                    try {
                        httpclient.close();
                    } catch (IOException ignored) {
                    }
                }
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

    private static String replaceCosHost(String jarPath, String cosL5Address) throws IOException {
        try {
            URL url = new URL(jarPath);
            String host = url.getHost();
            return jarPath.replace(host, cosL5Address);
        } catch (MalformedURLException e) {
            throw new IOException(e);
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
