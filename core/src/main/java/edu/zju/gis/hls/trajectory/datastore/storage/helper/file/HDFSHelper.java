package edu.zju.gis.hls.trajectory.datastore.storage.helper.file;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.Closeable;
import java.io.IOException;

/**
 * @author yanlo yanlong_lee@qq.com
 * @version 1.0 2018/07/17
 * Comments 用于读写HDFS的帮助类
 */
public final class HDFSHelper implements Closeable {

    private FileSystem dfs;

    public HDFSHelper(FileSystem dfs) {
        this.dfs = dfs;
    }

//    private static class InstanceHolder {
//        static HDFSHelper instance = new HDFSHelper(new Configuration());
//    }
//
//    public static HDFSHelper getInstance() {
//        return InstanceHolder.instance;
//    }

    /**
     * @param conf Hadoop配置，一般把$HADOOP_HOME/etc/hadoop/core-site.xml
     *             放在classpath下，通过{@link Configuration#Configuration()}生成
     */
    public HDFSHelper(Configuration conf) {
        try {
            conf.set("dfs.support.append", "true");
            conf.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER");
            conf.set("dfs.client.block.write.replace-datanode-on-failure.enable", "true");
            this.dfs = FileSystem.get(conf);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 判断路径是否存在
     *
     * @return true - 路径已存在
     */
    public boolean exists(Path path) throws IOException {
        return dfs.exists(path);
    }

    /**
     * 判断路径是否是文件
     */
    public boolean isFile(Path path) throws IOException {
        return dfs.isFile(path);
    }

    /**
     * 列出所有子路径，输入路径为文件时，直接返回该路径
     */
    public Path[] listFiles(Path path) throws IOException {
        return listFiles(path, null);
    }

    /**
     * 列出所有子路径，输入路径为文件时，直接返回该路径
     *
     * @param filter 路径过滤器，满足条件的路径被返回
     */
    public Path[] listFiles(Path path, PathFilter filter) throws IOException {
        FileStatus[] fileStatuses = filter == null ? dfs.listStatus(path) : dfs.listStatus(path, filter);
        Path[] paths = new Path[fileStatuses.length];
        for (int i = 0; i < fileStatuses.length; i++)
            paths[i] = fileStatuses[i].getPath();
        return paths;
    }

    /**
     * 新建目录
     *
     * @return true - 创建成功
     */
    public boolean mkdirs(Path dir) throws IOException {
        return dfs.mkdirs(dir);
    }

    /**
     * 新建空文件
     *
     * @return true - 创建成功，false - 文件已存在或创建失败
     */
    public boolean createFile(Path file) throws IOException {
        return dfs.createNewFile(file);
    }

    /**
     * @param path 待删除路径,当路径为目录时，请调用{@link #deletePath(Path, boolean)}，并将recursive设置为true
     * @return true - 删除成功
     */
    public boolean deletePath(Path path) throws IOException {
        return dfs.delete(path, false);
    }

    /**
     * @param path      待删除路径
     * @param recursive 当路径为目录时，设置为true可完全删除该目录，否则删除失败；当路径为文件时，该参数不影响删除的执行
     * @return true - 删除成功
     */
    public boolean deletePath(Path path, boolean recursive) throws IOException {
        return dfs.delete(path, recursive);
    }

    public FSDataInputStream read(Path path) throws IOException {
        return dfs.open(path);
    }

    /**
     * 写文件到HDFS
     *
     * @param path      目标文件
     * @param overwrite 是否覆盖已有文件
     */
    public FSDataOutputStream getFileOutStream(Path path, boolean overwrite) throws IOException {
        return dfs.create(path, overwrite);
    }

    /**
     * 追加内容到HDFS文件
     *
     * @param path 目标文件
     */
    public FSDataOutputStream append(Path path) throws IOException {
        return dfs.append(path);
    }

    /***
     * 上传文件
     * @param src 待上传文件
     * @param tar hdfs存储目录
     * @return 是否成功
     */
    public boolean copyFromLocalFile(Path src, Path tar) {
        try {
            dfs.copyFromLocalFile(src, tar);
            return true;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    /***
     * 下载文件
     * @param src hdfs存储目录
     * @param tar 下载位置
     * @return 是否成功
     */
    public boolean copyToLocalFile(Path src, Path tar) {
        try {
            dfs.copyToLocalFile(src, tar);
            return true;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }


    /***
     * 获取Home目录
     * @return
     */
    public String getHomeDirectory() {
        return dfs.getHomeDirectory().toString();
    }

    @Override
    public void close() throws IOException {
        dfs.close();
    }
}

