package com.amazonaws.kaja.samples;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class DepartmentsFlatMap implements FlatMapFunction<UserIdSessionEvent, Tuple2<String, Integer>> {
    @Override
    public void flatMap(UserIdSessionEvent value, Collector<Tuple2<String, Integer>> out) throws Exception {
        value.getDeptList().forEach(dept -> out.collect(new Tuple2<>(dept, 1)));
    }
}
