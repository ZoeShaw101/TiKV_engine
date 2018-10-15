package com.alibabacloud.polar_race.engine.common.lsmtree;

public class Run {
    private long maxSize;
    private double BFbitPerEntry;

    public Run(long maxSize, double BFbitPerEntry) {
        this.maxSize = maxSize;
        this.BFbitPerEntry = BFbitPerEntry;
    }

    public void write(Entry entry) {

    }
}
