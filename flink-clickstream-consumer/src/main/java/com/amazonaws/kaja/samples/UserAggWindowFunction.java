package com.amazonaws.kaja.samples;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class UserAggWindowFunction extends ProcessWindowFunction<ClickEventAggregate, UserIdSessionEvent, Integer, TimeWindow> {
    @Override
    public void process(Integer integer, Context context, Iterable<ClickEventAggregate> elements, Collector<UserIdSessionEvent> out) throws Exception {
        ClickEventAggregate element = elements.iterator().next();

        //out.collect(new Tuple7<>(element.f0, element.f1, element.f2, element.f3, element.f4, new Timestamp(context.window().getStart()), new Timestamp(context.window().getEnd())));

        out.collect(new UserIdSessionEvent()
                .setUserId(element.getUserId())
                .setEventCount((element.getEventCount()))
                .setOrderCheckoutEventCount((element.getEventCountWithOrderCheckout()))
                .setDeptList(element.getDepartmentsVisited())
                .setEventKey(element.getEventKey())
                .setWindowBeginTime(context.window().getStart())
                .setWindowEndTime(context.window().getEnd()));

    }
}
