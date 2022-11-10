package com.amazonaws.kaja.samples;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class DepartmentsAggWindowFunction extends ProcessWindowFunction<Tuple2<String, Integer>, DepartmentsAggEvent, String, TimeWindow> {
    @Override
    public void process(String key, Context context, Iterable<Tuple2<String, Integer>> elements, Collector<DepartmentsAggEvent> out) throws Exception {
        Tuple2<String, Integer> element = elements.iterator().next();

        out.collect(new DepartmentsAggEvent()
                .setDepartmentName(element.f0)
                .setDepartmentCount(element.f1)
                .setWindowBeginTime(context.window().getStart())
                .setWindowEndTime(context.window().getEnd()));
    }
}
