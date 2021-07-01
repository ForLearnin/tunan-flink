package com.tunan.flink.hadoop;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class MultiFileRecordWriter extends RecordWriter<Text, NullWritable> {

    FSDataOutputStream jdOut = null;
    FSDataOutputStream otherOut = null;
    FileSystem fileSystem = null;

    public MultiFileRecordWriter(TaskAttemptContext context) {

        try {
            fileSystem = FileSystem.get(context.getConfiguration());
            jdOut = fileSystem.create(new Path("tunan-flink-2021/out/jd.txt"));
            otherOut = fileSystem.create(new Path("tunan-flink-2021/out/other.txt"));
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @Override
    public void write(Text key, NullWritable value) throws IOException, InterruptedException {
        if(key.toString().contains("jd")){
            jdOut.write((key.toString()+"\n").getBytes());
        }else{
            otherOut.write((key.toString()+"\n").getBytes());
        }
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        IOUtils.closeStream(jdOut);
        IOUtils.closeStream(otherOut);
    }
}
