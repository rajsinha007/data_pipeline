package com.dhl.dppc.otlp;

/** Raw OTLP JSON message plus the labels derived from the topic it came from. */
public record TaggedPayload(String site, String region, String datacenter, String json) {
}
