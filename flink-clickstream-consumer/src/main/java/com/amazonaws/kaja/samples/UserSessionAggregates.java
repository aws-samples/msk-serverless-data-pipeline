package com.amazonaws.kaja.samples;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

public class UserSessionAggregates implements AggregateFunction<UserIdSessionEvent, Tuple2<Integer, Integer>, Tuple3<Integer, Integer, Integer>> {


    @Override
    public Tuple2<Integer, Integer> createAccumulator() {
        return new Tuple2<Integer, Integer>(0,0);
    }

    @Override
    public Tuple2<Integer, Integer> add(UserIdSessionEvent value, Tuple2<Integer, Integer> accumulator) {
        if (value.getOrderCheckoutEventCount() != 0){
            accumulator.f1++;
        }
        accumulator.f0++;
        return accumulator;
    }

    @Override
    public Tuple3<Integer, Integer, Integer> getResult(Tuple2<Integer, Integer> accumulator) {

        //System.out.printf("Trigger Timestamp:: %1$TD %1$TT %n", new Timestamp();
        return new Tuple3<>(accumulator.f0, accumulator.f1, accumulator.f1 * 100/(accumulator.f0));
    }

    @Override
    public Tuple2<Integer, Integer> merge(Tuple2<Integer, Integer> a, Tuple2<Integer, Integer> b) {
        a.f0 = a.f0 + b.f0;
        a.f1 = a.f1 + b.f1;
        return a;
    }
}
