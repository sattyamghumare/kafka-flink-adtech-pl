package org.adtech.consumer;

import org.adtech.avro.AdImpression;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;
import java.util.List;

public class FlinkAvroConsumer {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(10);
        env.getConfig().setAutoWatermarkInterval(0);

        KafkaSource<AdImpression> source = KafkaSource.<AdImpression>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("ad-impressions-avro")
                .setGroupId("flink-consumer-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new CustomAvroDeserializer())
                .setProperty("fetch.min.bytes", "102400")
                .setProperty("fetch.max.wait.ms", "500")
                .setProperty("partition.discovery.interval.ms", "30000")
                .build();

        DataStream<AdImpression> stream = env.fromSource(
                source,
                WatermarkStrategy.noWatermarks(),
                "Kafka Avro Source"
        ).setParallelism(10);

        // ============================================
        // TOTAL MESSAGE COUNT (Every 10 seconds)
        // FIXED: Use constant key, but with proper parallelism
        // ============================================
        stream
                .map(event -> 1L)
                .keyBy(x -> 0)  // Use integer key instead of string
                .process(new KeyedProcessFunction<Integer, Long, Long>() {
                    private long totalCount = 0;
                    private long lastPrintTime = 0;
                    private long lastCount = 0;

                    @Override
                    public void processElement(Long value, Context ctx, Collector<Long> out) {
                        totalCount++;
                        long currentTime = System.currentTimeMillis();

                        if (currentTime - lastPrintTime >= 10000) {
                            long messagesInWindow = totalCount - lastCount;
                            double rate = messagesInWindow * 1000.0 / (currentTime - lastPrintTime);

                            System.out.println("\n═══════════════════════════════════════════════════");
                            System.out.println("📊 [TOTAL] Messages in last 10 seconds: " + messagesInWindow);
                            System.out.println("📊 [TOTAL] Total messages so far: " + totalCount);
                            System.out.println("⚡ Processing Rate: " + String.format("%.0f", rate) + " msg/sec");
                            System.out.println("═══════════════════════════════════════════════════");

                            lastPrintTime = currentTime;
                            lastCount = totalCount;
                        };
                    }
                })
                .print();

        // ============================================
        // COUNTRY-WISE COUNT (Every 10 seconds)
        // ============================================
        stream
                .map(event -> Tuple2.of(event.getCountryCode().toString(), 1L))
                .returns(new TypeHint<Tuple2<String, Long>>() {})
                .keyBy(tuple -> tuple.f0)
                .process(new KeyedProcessFunction<String, Tuple2<String, Long>, String>() {
                    private Map<String, Long> countryCountMap = new HashMap<>();
                    private long lastPrintTime = 0;

                    @Override
                    public void open(Configuration parameters) {
                        lastPrintTime = System.currentTimeMillis();
                    }

                    @Override
                    public void processElement(Tuple2<String, Long> value, Context ctx, Collector<String> out) {
                        String country = value.f0;
                        countryCountMap.put(country, countryCountMap.getOrDefault(country, 0L) + 1);

                        long currentTime = System.currentTimeMillis();

                        if (currentTime - lastPrintTime >= 10000) {
                            long total = 0;
                            for (Long count : countryCountMap.values()) {
                                total += count;
                            }

                            StringBuilder sb = new StringBuilder();
                            sb.append("\n📍 COUNTRY WISE STATS (last 10 seconds):\n");
                            sb.append("───────────────────────────────────────────────────\n");

                            List<Map.Entry<String, Long>> entries = new ArrayList<>(countryCountMap.entrySet());
                            entries.sort((e1, e2) -> e2.getValue().compareTo(e1.getValue()));

                            for (Map.Entry<String, Long> entry : entries) {
                                double percentage = (entry.getValue() * 100.0 / total);
                                sb.append(String.format("   %-5s : %6d users (%.1f%%)\n",
                                        entry.getKey(), entry.getValue(), percentage));
                            }

                            sb.append("───────────────────────────────────────────────────\n");
                            sb.append(String.format("   TOTAL : %6d users\n", total));
                            sb.append("═══════════════════════════════════════════════════\n");

                            out.collect(sb.toString());
                            countryCountMap.clear();
                            lastPrintTime = currentTime;
                        }
                    }
                })
                .print();

        env.execute("Flink Avro Consumer - Real Time");
    }

    // Optimized Avro deserializer
    public static class CustomAvroDeserializer implements DeserializationSchema<AdImpression> {
        private transient SpecificDatumReader<AdImpression> reader;

        @Override
        public AdImpression deserialize(byte[] message) throws IOException {
            if (reader == null) {
                reader = new SpecificDatumReader<>(AdImpression.class);
            }
            BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(message, null);
            return reader.read(null, decoder);
        }

        @Override
        public boolean isEndOfStream(AdImpression nextElement) {
            return false;
        }

        @Override
        public TypeInformation<AdImpression> getProducedType() {
            return TypeInformation.of(AdImpression.class);
        }
    }
}