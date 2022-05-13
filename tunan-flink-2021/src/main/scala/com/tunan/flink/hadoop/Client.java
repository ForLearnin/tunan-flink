package com.tunan.flink.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;

import java.io.*;
import java.net.URI;

public class Client {

    private static FileSystem fileSystem = null;

    static {
        Configuration conf = new Configuration();
        conf.set("dfs.client.use.datanode.hostname", "true");
        conf.set("dfs.replication", "1");
        String user = "root";
        URI uri = null;
        try {
            uri = new URI("hdfs://aliyun:9000");
            fileSystem = FileSystem.get(uri, conf, user);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // 创建一个目录
    public static void createDir(String dir) throws IOException {
        fileSystem.mkdirs(new Path(dir));
    }

    // 创建一个文件
    public static void createFile(String file) throws IOException {
        fileSystem.create(new Path(file));
    }

    // 删除一个文件/目录
    public static void delete(String path) throws IOException {
        fileSystem.deleteOnExit(new Path(path));
    }

    // 上传一个文件
    public static void putFile(String src, String dst) throws IOException {
        fileSystem.copyFromLocalFile(new Path(src), new Path(dst));
    }

    // 下载一个文件
    public static void getFile(String src, String dst) throws IOException {
        fileSystem.copyToLocalFile(new Path(src), new Path(dst));
    }

    // 列出文件的状态
    public static void listFile(String path) throws IOException {
        RemoteIterator<LocatedFileStatus> files = fileSystem.listFiles(new Path(path), true);

        while (files.hasNext()) {
            LocatedFileStatus fileStatus = files.next();

            System.out.println(fileStatus.getPath());
            System.out.println(fileStatus.getBlockSize());
            System.out.println(fileStatus.getLen());
            BlockLocation[] locations = fileStatus.getBlockLocations();

            StringBuilder sb = new StringBuilder();
            for (BlockLocation location : locations) {
                sb.append(location).append("\t");
            }
            System.out.println(sb.toString());
            sb.delete(0, sb.length());
            System.out.println(" ========================== ");
        }

    }

    // 复制hdfs流到本地
    public static void copyFileStreamToLocal(String src, String dst) throws IOException {
        FSDataInputStream in = fileSystem.open(new Path(src));

        BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream(new File(dst)));

        int len;
        byte[] buffers = new byte[1024 * 8];

        while ((len = in.read(buffers)) != -1) {
            out.write(buffers, 0, len);
            out.flush();
        }

        // IOUtils.copyBytes(in,out,1024*8);

        IOUtils.closeStream(in);
        IOUtils.closeStream(out);
    }

    // 复制本地流到hdfs
    public static void copyFileStreamFromLocal(String src, String dst) throws IOException {
        BufferedInputStream in = new BufferedInputStream(new FileInputStream(new File(src)));
        FSDataOutputStream out = fileSystem.create(new Path(dst));

        int len = -1;
        byte[] bytes = new byte[1024 * 8];
        while ((len = in.read(bytes)) != -1) {
            out.write(bytes, 0, len);
            out.flush();
        }

        // IOUtils.copyBytes(in,out,1024*8);

        IOUtils.closeStream(in);
        IOUtils.closeStream(out);
    }

    public static void main(String[] args) {

        String dir = "/client";
        String file = "data.txt";
//        String src = "tunan-flink-2021/kafka-conf/parameters.properties";
//        String dst = "conf.properties";

        try {

//            delete(file);
//            createDir(dir);
//            createFile(dir+"/"+file);
//            putFile(src,dir+"/"+dst);
//            getFile(dir+"/"+dst,"tunan-flink-2021/kafka-conf/");

//            listFile(dir);
//            copyFileStreamToLocal(dir+"/"+dst,"tunan-flink-2021/kafka-conf/stream.properties");
            copyFileStreamFromLocal("tunan-flink-2021/data.txt", "/client/wc.txt");

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
