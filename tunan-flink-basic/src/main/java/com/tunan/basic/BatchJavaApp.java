package com.tunan.basic;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class BatchJavaApp {
    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<String> file = env.readTextFile("tunan-flink-basic/data/word.txt");
        file
                .flatMap(new MyFlatMapFunction())
                .map(new MyMapFunction())
                .groupBy(0)
                .sum(1)
                .print();
    }
}

class MyFlatMapFunction implements FlatMapFunction<String, String> {

    @Override
    public void flatMap(String s, Collector<String> collector) throws Exception {
        String[] words = s.toLowerCase().split(",");
        for (String word : words) {
            collector.collect(word);
        }
    }
};

class MyMapFunction implements MapFunction<String, Tuple2<String, Integer>> {
    @Override
    public Tuple2<String, Integer> map(String s) throws Exception {
        return Tuple2.of(s, 1);
    }
}