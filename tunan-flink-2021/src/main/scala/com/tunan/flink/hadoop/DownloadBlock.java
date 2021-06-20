package com.tunan.flink.hadoop;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

public class DownloadBlock {

    // 下载第一个块
    public static void getBlock1(FileSystem fileSystem, String src, String dst) throws IOException {
        FSDataInputStream in = fileSystem.open(new Path(src));
        BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream(new File(dst)));

        byte[] bytes = new byte[1024 * 8];

        for (int i = 0; i < 1024 * 128; i++) {
            in.read(bytes);
            out.write(bytes);
        }

        IOUtils.closeStream(in);
        IOUtils.closeStream(out);
    }

    // 下载第二个块
    public static void getBlock2(FileSystem fileSystem, String src, String dst) throws IOException {
        FSDataInputStream in = fileSystem.open(new Path(src));
        BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream(new File(dst)));
        in.seek(1024 * 1024 * 128);

        byte[] bytes = new byte[1024 * 8];

        for (int i = 0; i < 1024 * 128; i++) {
            in.read(bytes);
            out.write(bytes);
        }

        IOUtils.closeStream(in);
        IOUtils.closeStream(out);
    }

    // 下载第最后一个块
    public static void getBlock3(FileSystem fileSystem, String src, String dst) throws IOException {
        FSDataInputStream in = fileSystem.open(new Path(src));
        BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream(new File(dst)));
        in.seek(1024 * 1024 * 128 * 2);

        // 复制流
        IOUtils.copyBytes(in, out, 1024 * 8);

        IOUtils.closeStream(in);
        IOUtils.closeStream(out);
    }

}
