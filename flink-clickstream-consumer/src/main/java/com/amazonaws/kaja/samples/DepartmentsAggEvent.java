package com.amazonaws.kaja.samples;

public class DepartmentsAggEvent {
    private String departmentName;
    private Integer departmentCount;
    private Long windowBeginTime;
    private Long windowEndTime;

    public String getDepartmentName() {
        return departmentName;
    }

    public DepartmentsAggEvent setDepartmentName(String departmentName) {
        this.departmentName = departmentName;
        return this;
    }

    public Integer getDepartmentCount() {
        return departmentCount;
    }

    public DepartmentsAggEvent setDepartmentCount(Integer departmentCount) {
        this.departmentCount = departmentCount;
        return this;
    }

    public Long getWindowBeginTime() {
        return windowBeginTime;
    }

    public DepartmentsAggEvent setWindowBeginTime(Long windowBegin) {
        this.windowBeginTime = windowBegin;
        return this;
    }

    public Long getWindowEndTime() {
        return windowEndTime;
    }

    public DepartmentsAggEvent setWindowEndTime(Long windowEnd) {
        this.windowEndTime = windowEnd;
        return this;
    }
}
