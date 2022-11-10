package com.amazonaws.kaja.samples;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class UserSessionWindowFunction extends ProcessWindowFunction<Tuple3<Integer, Integer, Integer>, UserIdAggEvent, Integer, TimeWindow> {

    @Override
    public void process(Integer key, Context context, Iterable<Tuple3<Integer, Integer, Integer>> elements, Collector<UserIdAggEvent> out) {
        Tuple3<Integer, Integer, Integer> element = elements.iterator().next();

        out.collect(new UserIdAggEvent()
                .setUserSessionCount(element.f0)
                .setUserSessionCountWithOrderCheckout(element.f1)
                .setPercentSessionswithBuy(element.f2)
                .setWindowBeginTime(context.window().getStart())
                .setWindowEndTime(context.window().getEnd()));
    }
}
