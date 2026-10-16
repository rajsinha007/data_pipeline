package com.dhl.dppc.otlp;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.metrics.v1.AggregationTemporality;
import io.opentelemetry.proto.metrics.v1.HistogramDataPoint;
import io.opentelemetry.proto.metrics.v1.Metric;
import io.opentelemetry.proto.metrics.v1.NumberDataPoint;
import io.opentelemetry.proto.metrics.v1.ResourceMetrics;
import io.opentelemetry.proto.metrics.v1.ScopeMetrics;
import io.opentelemetry.proto.metrics.v1.SummaryDataPoint;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Converts an OTLP JSON metrics export into Prometheus samples with region/datacenter/site labels. */
public class OtlpMetricsParser extends RichFlatMapFunction<TaggedPayload, PromSample> {

    private static final Logger LOG = LoggerFactory.getLogger(OtlpMetricsParser.class);
    private static final int FLAG_NO_RECORDED_VALUE = 1;

    private transient JsonFormat.Parser parser;

    @Override
    public void open(OpenContext openContext) {
        parser = JsonFormat.parser().ignoringUnknownFields();
    }

    @Override
    public void flatMap(TaggedPayload in, Collector<PromSample> out) {
        ExportMetricsServiceRequest.Builder request = ExportMetricsServiceRequest.newBuilder();
        try {
            parser.merge(in.json(), request);
        } catch (InvalidProtocolBufferException e) {
            LOG.warn("Dropping unparsable OTLP JSON from site {}: {}", in.site(), e.getMessage());
            return;
        }

        for (ResourceMetrics rm : request.getResourceMetricsList()) {
            Map<String, String> resourceLabels = new TreeMap<>();
            putAttributes(resourceLabels, rm.getResource().getAttributesList());
            for (ScopeMetrics sm : rm.getScopeMetricsList()) {
                for (Metric metric : sm.getMetricsList()) {
                    convert(metric, resourceLabels, in, out);
                }
            }
        }
    }

    private void convert(Metric m, Map<String, String> resourceLabels, TaggedPayload in, Collector<PromSample> out) {
        String name = sanitizeName(m.getName());
        switch (m.getDataCase()) {
            case GAUGE -> numbers(name, m.getGauge().getDataPointsList(), resourceLabels, in, out);
            case SUM -> {
                if (m.getSum().getAggregationTemporality() != AggregationTemporality.AGGREGATION_TEMPORALITY_CUMULATIVE) {
                    return;
                }
                String n = m.getSum().getIsMonotonic() && !name.endsWith("_total") ? name + "_total" : name;
                numbers(n, m.getSum().getDataPointsList(), resourceLabels, in, out);
            }
            case HISTOGRAM -> {
                if (m.getHistogram().getAggregationTemporality() != AggregationTemporality.AGGREGATION_TEMPORALITY_CUMULATIVE) {
                    return;
                }
                for (HistogramDataPoint dp : m.getHistogram().getDataPointsList()) {
                    histogram(name, dp, resourceLabels, in, out);
                }
            }
            case SUMMARY -> {
                for (SummaryDataPoint dp : m.getSummary().getDataPointsList()) {
                    summary(name, dp, resourceLabels, in, out);
                }
            }
            default -> {
                // Exponential histograms and empty metrics have no classic Prometheus representation.
            }
        }
    }

    private void numbers(String name, List<NumberDataPoint> points, Map<String, String> resourceLabels,
                         TaggedPayload in, Collector<PromSample> out) {
        for (NumberDataPoint dp : points) {
            if ((dp.getFlags() & FLAG_NO_RECORDED_VALUE) != 0) {
                continue;
            }
            double v;
            switch (dp.getValueCase()) {
                case AS_DOUBLE -> v = dp.getAsDouble();
                case AS_INT -> v = dp.getAsInt();
                default -> {
                    continue;
                }
            }
            emit(out, name, labels(resourceLabels, dp.getAttributesList(), in), null, null, v, dp.getTimeUnixNano());
        }
    }

    private void histogram(String name, HistogramDataPoint dp, Map<String, String> resourceLabels,
                           TaggedPayload in, Collector<PromSample> out) {
        if ((dp.getFlags() & FLAG_NO_RECORDED_VALUE) != 0) {
            return;
        }
        Map<String, String> base = labels(resourceLabels, dp.getAttributesList(), in);
        long ts = dp.getTimeUnixNano();
        List<Long> counts = dp.getBucketCountsList();
        List<Double> bounds = dp.getExplicitBoundsList();
        long cumulative = 0;
        for (int i = 0; i < counts.size(); i++) {
            cumulative += counts.get(i);
            String le = i < bounds.size() ? formatBound(bounds.get(i)) : "+Inf";
            emit(out, name + "_bucket", base, "le", le, cumulative, ts);
        }
        if (dp.hasSum()) {
            emit(out, name + "_sum", base, null, null, dp.getSum(), ts);
        }
        emit(out, name + "_count", base, null, null, dp.getCount(), ts);
    }

    private void summary(String name, SummaryDataPoint dp, Map<String, String> resourceLabels,
                         TaggedPayload in, Collector<PromSample> out) {
        if ((dp.getFlags() & FLAG_NO_RECORDED_VALUE) != 0) {
            return;
        }
        Map<String, String> base = labels(resourceLabels, dp.getAttributesList(), in);
        long ts = dp.getTimeUnixNano();
        for (SummaryDataPoint.ValueAtQuantile q : dp.getQuantileValuesList()) {
            emit(out, name, base, "quantile", formatBound(q.getQuantile()), q.getValue(), ts);
        }
        emit(out, name + "_sum", base, null, null, dp.getSum(), ts);
        emit(out, name + "_count", base, null, null, dp.getCount(), ts);
    }

    private Map<String, String> labels(Map<String, String> resourceLabels, List<KeyValue> attrs, TaggedPayload in) {
        Map<String, String> labels = new TreeMap<>(resourceLabels);
        putAttributes(labels, attrs);
        labels.put("region", in.region());
        labels.put("datacenter", in.datacenter());
        labels.put("site", in.site());
        return labels;
    }

    private void emit(Collector<PromSample> out, String metricName, Map<String, String> base,
                      String extraName, String extraValue, double value, long timeUnixNano) {
        TreeMap<String, String> all = new TreeMap<>(base);
        all.put("__name__", metricName);
        if (extraName != null) {
            all.put(extraName, extraValue);
        }
        String[] flat = new String[all.size() * 2];
        StringBuilder key = new StringBuilder();
        int i = 0;
        for (Map.Entry<String, String> e : all.entrySet()) {
            flat[i++] = e.getKey();
            flat[i++] = e.getValue();
            key.append(e.getKey()).append('\u0000').append(e.getValue()).append('\u0001');
        }
        long ts = timeUnixNano > 0 ? timeUnixNano / 1_000_000L : System.currentTimeMillis();
        out.collect(new PromSample(key.toString(), flat, value, ts));
    }

    private static void putAttributes(Map<String, String> target, List<KeyValue> attrs) {
        for (KeyValue kv : attrs) {
            AnyValue v = kv.getValue();
            String s = switch (v.getValueCase()) {
                case STRING_VALUE -> v.getStringValue();
                case BOOL_VALUE -> Boolean.toString(v.getBoolValue());
                case INT_VALUE -> Long.toString(v.getIntValue());
                case DOUBLE_VALUE -> Double.toString(v.getDoubleValue());
                default -> null;
            };
            if (s != null) {
                target.put(sanitizeLabel(kv.getKey()), s);
            }
        }
    }

    static String sanitizeName(String s) {
        return sanitize(s, true);
    }

    static String sanitizeLabel(String s) {
        return sanitize(s, false);
    }

    private static String sanitize(String s, boolean allowColon) {
        StringBuilder b = new StringBuilder(s.length() + 1);
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            boolean ok = (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_'
                    || (allowColon && c == ':');
            b.append(ok ? c : '_');
        }
        if (b.length() == 0 || (b.charAt(0) >= '0' && b.charAt(0) <= '9')) {
            b.insert(0, '_');
        }
        return b.toString();
    }

    private static String formatBound(double d) {
        if (d == Math.rint(d) && Math.abs(d) < 1e15) {
            return Long.toString((long) d);
        }
        return Double.toString(d);
    }
}
