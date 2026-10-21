package com.dhl.dppc.otlp;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

/** Kafka clusters and the sites (OTel source clusters) read from them. */
public final class Sites implements Serializable {

    public static final String TOPIC_PREFIX = "otel.metrics.";

    public record Site(String id, String kafka, String region, String datacenter) implements Serializable {
        public String topic() {
            return TOPIC_PREFIX + id;
        }
    }

    private final Map<String, String> bootstrapByKafka = new LinkedHashMap<>();
    private final Map<String, Site> siteById = new LinkedHashMap<>();

    public static Sites load() throws IOException {
        Properties p = new Properties();
        try (InputStream in = Sites.class.getResourceAsStream("/sites.properties")) {
            if (in == null) {
                throw new IOException("sites.properties not found on classpath");
            }
            p.load(in);
        }
        Sites s = new Sites();
        for (String key : p.stringPropertyNames()) {
            if (key.startsWith("kafka.") && key.endsWith(".bootstrap")) {
                s.bootstrapByKafka.put(key.substring(6, key.length() - 10), p.getProperty(key).trim());
            }
        }
        for (String key : p.stringPropertyNames()) {
            if (key.startsWith("site.") && key.endsWith(".kafka")) {
                String id = key.substring(5, key.length() - 6);
                String kafka = p.getProperty(key).trim();
                if (!s.bootstrapByKafka.containsKey(kafka)) {
                    throw new IOException("Site " + id + " references unknown kafka cluster " + kafka);
                }
                s.siteById.put(id, new Site(id, kafka,
                        required(p, "site." + id + ".region"),
                        required(p, "site." + id + ".datacenter")));
            }
        }
        return s;
    }

    private static String required(Properties p, String key) throws IOException {
        String v = p.getProperty(key);
        if (v == null || v.isBlank()) {
            throw new IOException("Missing " + key + " in sites.properties");
        }
        return v.trim();
    }

    public Iterable<String> kafkaIds() {
        return bootstrapByKafka.keySet();
    }

    public String bootstrap(String kafka) {
        return bootstrapByKafka.get(kafka);
    }

    /** Sites on one Kafka cluster, keyed by topic name. */
    public HashMap<String, Site> sitesByTopic(String kafka) {
        HashMap<String, Site> m = new HashMap<>();
        for (Site site : siteById.values()) {
            if (site.kafka().equals(kafka)) {
                m.put(site.topic(), site);
            }
        }
        return m;
    }
}
