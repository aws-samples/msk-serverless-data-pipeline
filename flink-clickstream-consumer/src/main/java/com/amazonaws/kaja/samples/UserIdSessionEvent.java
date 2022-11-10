package com.amazonaws.kaja.samples;

import java.util.HashSet;

public class UserIdSessionEvent {
    private long userId;
    private Integer eventCount;
    private Integer orderCheckoutEventCount;
    private HashSet<String> deptList;
    private Integer eventKey;
    private Long windowBeginTime;
    private Long windowEndTime;

    @Override
    public String toString() {
        return "UserIdSessionEvent{" +
                "userId=" + userId +
                ", eventCount=" + eventCount +
                ", orderCheckoutEventCount=" + orderCheckoutEventCount +
                ", deptList=" + deptList +
                ", eventKey=" + eventKey +
                ", windowBeginTime=" + windowBeginTime +
                ", windowEndTime=" + windowEndTime +
                '}';
    }



    public HashSet<String> getDeptList() {
        return deptList;
    }

    public UserIdSessionEvent setDeptList(HashSet<String> deptList) {
        this.deptList = deptList;
        return this;
    }

    public Integer getEventKey() {
        return eventKey;
    }

    public UserIdSessionEvent setEventKey(Integer eventKey) {
        this.eventKey = eventKey;
        return this;
    }

    public Long getWindowBeginTime() {
        return windowBeginTime;
    }

    public UserIdSessionEvent setWindowBeginTime(Long windowBeginTime) {
        this.windowBeginTime = windowBeginTime;
        return this;
    }

    public Long getWindowEndTime() {
        return windowEndTime;
    }

    public UserIdSessionEvent setWindowEndTime(Long windowEndTime) {
        this.windowEndTime = windowEndTime;
        return this;
    }

    public long getUserId() {
        return userId;
    }

    public UserIdSessionEvent setUserId(long userId) {
        this.userId = userId;
        return this;
    }

    public Integer getEventCount() {
        return eventCount;
    }

    public UserIdSessionEvent setEventCount(Integer eventCount) {
        this.eventCount = eventCount;
        return this;
    }

    public Integer getOrderCheckoutEventCount() {
        return orderCheckoutEventCount;
    }

    public UserIdSessionEvent setOrderCheckoutEventCount(Integer orderCheckoutEventCount) {
        this.orderCheckoutEventCount = orderCheckoutEventCount;
        return this;
    }


}
