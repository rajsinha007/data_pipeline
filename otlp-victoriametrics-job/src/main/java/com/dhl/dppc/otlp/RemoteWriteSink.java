package com.dhl.dppc.otlp;

import com.google.protobuf.CodedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.Snappy;

/**
 * Minimal Prometheus remote_write (v1) sink for VictoriaMetrics. Buffers samples and sends them on batch size and on every
 * checkpoint, so delivery is at-least-once. Server errors are retried; other 4xx responses drop the batch.
 */
public class RemoteWriteSink implements Sink<PromSample> {

    private final String url;
    private final int maxBatchSamples;
    private final int maxAttempts;

    public RemoteWriteSink(String url, int maxBatchSamples, int maxAttempts) {
        this.url = url;
        this.maxBatchSamples = maxBatchSamples;
        this.maxAttempts = maxAttempts;
    }

    @Override
    public SinkWriter<PromSample> createWriter(WriterInitContext context) {
        return new Writer(url, maxBatchSamples, maxAttempts);
    }

    private static final class Writer implements SinkWriter<PromSample> {

        private static final Logger LOG = LoggerFactory.getLogger(Writer.class);

        private final URI uri;
        private final int maxBatchSamples;
        private final int maxAttempts;
        private final HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
        private final List<PromSample> buffer = new ArrayList<>();

        Writer(String url, int maxBatchSamples, int maxAttempts) {
            this.uri = URI.create(url);
            this.maxBatchSamples = maxBatchSamples;
            this.maxAttempts = maxAttempts;
        }

        @Override
        public void write(PromSample sample, Context context) throws IOException, InterruptedException {
            buffer.add(sample);
            if (buffer.size() >= maxBatchSamples) {
                flush(false);
            }
        }

        @Override
        public void flush(boolean endOfInput) throws IOException, InterruptedException {
            if (buffer.isEmpty()) {
                return;
            }
            Map<String, List<PromSample>> bySeries = new LinkedHashMap<>();
            for (PromSample s : buffer) {
                bySeries.computeIfAbsent(s.seriesKey(), k -> new ArrayList<>()).add(s);
            }
            byte[] body = Snappy.compress(encode(bySeries.values()));
            send(body, buffer.size());
            buffer.clear();
        }

        private void send(byte[] body, int sampleCount) throws IOException, InterruptedException {
            HttpRequest request = HttpRequest.newBuilder(uri)
                    .timeout(Duration.ofSeconds(30))
                    .header("Content-Type", "application/x-protobuf")
                    .header("Content-Encoding", "snappy")
                    .header("X-Prometheus-Remote-Write-Version", "0.1.0")
                    .POST(HttpRequest.BodyPublishers.ofByteArray(body))
                    .build();

            long backoffMs = 1000;
            for (int attempt = 1; ; attempt++) {
                String failure;
                try {
                    HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
                    int status = response.statusCode();
                    if (status / 100 == 2) {
                        return;
                    }
                    if (status / 100 == 4 && status != 429) {
                        LOG.warn("VictoriaMetrics rejected {} samples with HTTP {}, dropping batch: {}",
                                sampleCount, status, response.body());
                        return;
                    }
                    failure = "HTTP " + status + ": " + response.body();
                } catch (IOException e) {
                    failure = e.toString();
                }
                if (attempt >= maxAttempts) {
                    throw new IOException("Remote write failed after " + attempt + " attempts: " + failure);
                }
                LOG.warn("Remote write attempt {} failed ({}), retrying in {} ms", attempt, failure, backoffMs);
                Thread.sleep(backoffMs);
                backoffMs = Math.min(backoffMs * 2, 30_000);
            }
        }

        // WriteRequest { repeated TimeSeries timeseries = 1; }
        private static byte[] encode(Iterable<List<PromSample>> series) throws IOException {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            CodedOutputStream w = CodedOutputStream.newInstance(bytes);
            for (List<PromSample> s : series) {
                w.writeByteArray(1, timeSeries(s));
            }
            w.flush();
            return bytes.toByteArray();
        }

        // TimeSeries { repeated Label labels = 1; repeated Sample samples = 2; } with samples in time order
        private static byte[] timeSeries(List<PromSample> samples) throws IOException {
            samples.sort(Comparator.comparingLong(PromSample::timestampMs));
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            CodedOutputStream w = CodedOutputStream.newInstance(bytes);
            String[] labels = samples.get(0).labels();
            for (int i = 0; i < labels.length; i += 2) {
                ByteArrayOutputStream lb = new ByteArrayOutputStream();
                CodedOutputStream lw = CodedOutputStream.newInstance(lb);
                lw.writeString(1, labels[i]);
                lw.writeString(2, labels[i + 1]);
                lw.flush();
                w.writeByteArray(1, lb.toByteArray());
            }
            for (PromSample s : samples) {
                ByteArrayOutputStream sb = new ByteArrayOutputStream();
                CodedOutputStream sw = CodedOutputStream.newInstance(sb);
                sw.writeDouble(1, s.value());
                sw.writeInt64(2, s.timestampMs());
                sw.flush();
                w.writeByteArray(2, sb.toByteArray());
            }
            w.flush();
            return bytes.toByteArray();
        }

        @Override
        public void close() {
            // Unflushed samples are replayed from the last checkpoint.
        }
    }
}
