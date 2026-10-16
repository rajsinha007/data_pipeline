package com.dhl.dppc.otlp;

/** One sample of one series. {@code labels} is a flat, name-sorted [name0, value0, name1, value1, ...] array. */
public record PromSample(String seriesKey, String[] labels, double value, long timestampMs) {
}
