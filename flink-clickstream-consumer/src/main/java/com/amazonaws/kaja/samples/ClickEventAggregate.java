package com.amazonaws.kaja.samples;

import java.util.HashSet;

public class ClickEventAggregate {
    private Integer eventCount = 0;
    private Integer eventCountWithOrderCheckout = 0;
    private HashSet<String> departmentsVisited = new HashSet<>(0);
    private Integer userId = 0;
    private Integer eventKey = 0;

    public Integer getEventKey() {
        return eventKey;
    }

    public void setEventKey(Integer eventKey) {
        this.eventKey = eventKey;
    }

    public Integer getUserId() {
        return userId;
    }

    public void setUserId(Integer userId) {
        this.userId = userId;
    }

    public ClickEventAggregate(){}

    public Integer getEventCount() {
        return eventCount;
    }

    public void setEventCount(Integer eventCount) {
        this.eventCount = eventCount;
    }

    public Integer getEventCountWithOrderCheckout() {
        return eventCountWithOrderCheckout;
    }

    public void setEventCountWithOrderCheckout(Integer eventCountWithOrderCheckout) {
        this.eventCountWithOrderCheckout = eventCountWithOrderCheckout;
    }

    public HashSet<String> getDepartmentsVisited() {
        return departmentsVisited;
    }

    public void setDepartmentsVisited(HashSet<String> departmentsVisited) {
        this.departmentsVisited = departmentsVisited;
    }
}
