package com.tunan.basic;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class StreamJavaApp {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> stream = env.socketTextStream("aliyun", 9999);

        stream
                .flatMap(new MyStreamFlatMapFunction())
                .filter(new MyStreamFilterFunction())
                .map(new MyStreamMapFunction())
                .keyBy(new MyStreamKeySelector())
                .sum(1)
                .print()
        ;

        env.execute("StreamJavaApp");
    }
}

class MyStreamKeySelector implements KeySelector<Tuple2<String, Integer>, String> {
    @Override
    public String getKey(Tuple2<String, Integer> value) throws Exception {
        return value.f0;
    }
}


class MyStreamFilterFunction implements FilterFunction<String> {
    @Override
    public boolean filter(String value) throws Exception {
        return !value.isEmpty();
    }
}


class MyStreamFlatMapFunction implements FlatMapFunction<String, String> {

    @Override
    public void flatMap(String s, Collector<String> collector) throws Exception {
        String[] words = s.toLowerCase().split(",");
        for (String word : words) {
            collector.collect(word);
        }
    }
};

class MyStreamMapFunction implements MapFunction<String, Tuple2<String, Integer>> {
    @Override
    public Tuple2<String, Integer> map(String s) throws Exception {
        return Tuple2.of(s, 1);
    }
}