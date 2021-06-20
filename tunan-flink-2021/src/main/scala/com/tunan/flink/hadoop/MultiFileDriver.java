package com.tunan.flink.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;


class MultiFileDriver extends Configured implements Tool {


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        int run = ToolRunner.run(conf, new MultiFileDriver(), args);
        System.exit(run);
    }

    @Override
    public int run(String[] args) throws Exception {
        String in = "tunan-flink-2021/data/domain.txt";
        String out = "tunan-flink-2021/out";


        Configuration conf = super.getConf();

        Job job = Job.getInstance(conf);
        job.setJarByClass(MultiFileDriver.class);

        job.setMapperClass(MultiFileMapper.class);
        job.setReducerClass(MultiFileReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        job.setOutputFormatClass(MultiFileOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(in));
        FileOutputFormat.setOutputPath(job, new Path(out));

        boolean b = job.waitForCompletion(true);
        return b ? 0 : 1;
    }


    public static class MultiFileMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            context.write(value,NullWritable.get());
        }
    }

    public static class MultiFileReducer extends Reducer<Text, NullWritable, Text, NullWritable> {
        @Override
        protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            for (NullWritable value : values) {
                context.write(key, value);
            }
        }
    }
}
