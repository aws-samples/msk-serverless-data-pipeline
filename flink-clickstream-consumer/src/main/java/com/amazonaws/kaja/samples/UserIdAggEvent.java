package com.amazonaws.kaja.samples;

public class UserIdAggEvent {
    private Integer userSessionCount;
    private Integer userSessionCountWithOrderCheckout;
    private double percentSessionswithBuy;
    private Long windowBeginTime;
    private Long windowEndTime;

    public Integer getUserSessionCount() {
        return userSessionCount;
    }

    public UserIdAggEvent setUserSessionCount(Integer userSessionCount) {
        this.userSessionCount = userSessionCount;
        return this;
    }

    public Integer getUserSessionCountWithOrderCheckout() {
        return userSessionCountWithOrderCheckout;
    }

    public UserIdAggEvent setUserSessionCountWithOrderCheckout(Integer userSessionCountWithOrderCheckout) {
        this.userSessionCountWithOrderCheckout = userSessionCountWithOrderCheckout;
        return this;
    }

    public double getPercentSessionswithBuy() {
        return percentSessionswithBuy;
    }

    public UserIdAggEvent setPercentSessionswithBuy(double percentSessionswithBuy) {
        this.percentSessionswithBuy = percentSessionswithBuy;
        return this;
    }

    public Long getWindowBeginTime() {
        return windowBeginTime;
    }

    public UserIdAggEvent setWindowBeginTime(Long windowBeginTime) {
        this.windowBeginTime = windowBeginTime;
        return this;
    }

    public Long getWindowEndTime() {
        return windowEndTime;
    }

    public UserIdAggEvent setWindowEndTime(Long windowEndTime) {
        this.windowEndTime = windowEndTime;
        return this;
    }
}
