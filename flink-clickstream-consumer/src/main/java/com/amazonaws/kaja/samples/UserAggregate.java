package com.amazonaws.kaja.samples;

import org.apache.flink.api.common.functions.AggregateFunction;
import samples.clickstream.avro.ClickEvent;

import java.util.HashSet;

public class UserAggregate implements AggregateFunction<ClickEvent, ClickEventAggregate, ClickEventAggregate> {
    transient HashSet<String> departmentsVisited;

    @Override
    public ClickEventAggregate createAccumulator() {
        return new ClickEventAggregate();
    }

    @Override
    public ClickEventAggregate add(ClickEvent value, ClickEventAggregate accumulator) {

        if (!(value.getProductType().toString().equals("")) && !(value.getProductType().toString().equals("N/A"))) {
            accumulator.setEventCount(accumulator.getEventCount() + 1);
            departmentsVisited = accumulator.getDepartmentsVisited();
            departmentsVisited.add(value.getProductType().toString());
            accumulator.setDepartmentsVisited(departmentsVisited);
        }

        if (accumulator.getUserId() == 0) {
            accumulator.setUserId(value.getUserid());
        }

        if (value.getEventType().toString().equals("order_checkout")) {
            accumulator.setEventCountWithOrderCheckout(accumulator.getEventCount());
            //System.out.printf("Accumulator event count: %d %n", accumulator.getEventCountWithOrderCheckout());
        }
        //System.out.printf("UsedId: %d, Accumulator event count: %d %n", accumulator.getUserId(), accumulator.getEventCount());
        accumulator.setEventKey(1);

        return accumulator;
    }

    @Override
    public ClickEventAggregate getResult(ClickEventAggregate accumulator) {
        return accumulator;
    }

    @Override
    public ClickEventAggregate merge(ClickEventAggregate a, ClickEventAggregate b) {
        a.setEventCount(a.getEventCount() + b.getEventCount());
        a.setEventCountWithOrderCheckout(a.getEventCountWithOrderCheckout() + b.getEventCountWithOrderCheckout());
        departmentsVisited = a.getDepartmentsVisited();
        departmentsVisited.addAll(b.getDepartmentsVisited());
        a.setDepartmentsVisited(departmentsVisited);
        return a;
    }
}
