package org.adtech.producer;

import org.adtech.avro.AdImpression;
import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.UUID;

public class AvroAdImpressionProducer {

    private static final String TOPIC = "ad-impressions-avro";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final int EVENTS_PER_SECOND = 100;
    private static final int PRODUCER_THREADS = 4;

    private final KafkaProducer<String, byte[]> producer;
    private final AtomicLong sentCount = new AtomicLong();
    private volatile boolean running = true;

    public AvroAdImpressionProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 5);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 32768);
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        // 🔥 REMOVED: ALLOW_AUTO_CREATE_TOPICS_CONFIG

        this.producer = new KafkaProducer<>(props);
    }

    public void start() {
        System.out.println("🚀 Avro Producer Started (No Schema Registry)");

        ExecutorService executor = Executors.newFixedThreadPool(PRODUCER_THREADS);

        for (int i = 0; i < PRODUCER_THREADS; i++) {
            final int threadId = i;
            executor.submit(() -> produceEvents(threadId));
        }

        ScheduledExecutorService metricsExec = Executors.newSingleThreadScheduledExecutor();
        metricsExec.scheduleAtFixedRate(this::printMetrics, 5, 5, TimeUnit.SECONDS);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            running = false;
            executor.shutdown();
            metricsExec.shutdown();
            producer.flush();
            producer.close();
            System.out.println("✅ Producer closed. Total sent: " + sentCount.get());
        }));
    }

    private void produceEvents(int threadId) {
        while (running) {
            try {
                AdImpression event = AdImpression.newBuilder()
                        .setEventId(UUID.randomUUID().toString())
                        .setEventType(randomEventType())
                        .setTimestamp(System.currentTimeMillis())
                        .setUserId("user_" + (int)(Math.random() * 10000))
                        .setPublisherName(randomPublisher())
                        .setAdvertiserName(randomAdvertiser())
                        .setCampaignId("camp_" + (int)(Math.random() * 100))
                        .setBidPrice(Math.round((Math.random() * 10 + 0.5) * 100) / 100.0)
                        .setDeviceType(randomDevice())
                        .setIpAddress(randomIp())
                        .setCountryCode(randomCountry())
                        .setCity(randomCity())
                        .setSessionId("sess_" + UUID.randomUUID().toString().substring(0, 8))
                        .setAdPosition(randomPosition())
                        .setIsSkippable(Math.random() > 0.5)
                        .setTimeWatchedSeconds((int)(Math.random() * 30))
                        .build();

                byte[] avroBytes = serializeAvro(event);

                ProducerRecord<String, byte[]> record = new ProducerRecord<>(TOPIC, event.getUserId().toString(), avroBytes);

                producer.send(record, (metadata, exception) -> {
                    if (exception == null) {
                        sentCount.incrementAndGet();
                        if (sentCount.get() % 100 == 0) {
                            System.out.printf("✓ Sent: %d events (partition=%d, offset=%d)%n",
                                    sentCount.get(), metadata.partition(), metadata.offset());
                        }
                    } else {
                        System.err.println("✗ Error: " + exception.getMessage());
                    }
                });

                long sleepMs = (1000L * PRODUCER_THREADS / EVENTS_PER_SECOND);
                Thread.sleep(sleepMs);

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (IOException e) {
                System.err.println("Serialization error: " + e.getMessage());
            }
        }
    }

    private byte[] serializeAvro(AdImpression event) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        DatumWriter<AdImpression> writer = new SpecificDatumWriter<>(AdImpression.class);
        writer.write(event, encoder);
        encoder.flush();
        return out.toByteArray();
    }

    private void printMetrics() {
        System.out.printf("📊 Total Avro events sent: %d%n", sentCount.get());
    }

    private String randomEventType() {
        double r = Math.random();
        if (r < 0.85) return "IMPRESSION";
        if (r < 0.97) return "CLICK";
        return "CONVERSION";
    }

    private String randomPublisher() {
        String[] pubs = {"Hotstar", "Netflix", "Amazon Prime", "Sony LIV", "Zee5"};
        return pubs[(int)(Math.random() * pubs.length)];
    }

    private String randomAdvertiser() {
        String[] ads = {"Nike", "Pepsi", "Amazon", "Coca-Cola", "Apple", "Samsung", "McDonald's"};
        return ads[(int)(Math.random() * ads.length)];
    }

    private String randomDevice() {
        String[] devices = {"Mobile", "Desktop", "TV", "Tablet"};
        return devices[(int)(Math.random() * devices.length)];
    }

    private String randomIp() {
        return (int)(Math.random() * 255) + "." + (int)(Math.random() * 255) + "." +
                (int)(Math.random() * 255) + "." + (int)(Math.random() * 255);
    }

    private String randomCountry() {
        String[] countries = {"IN", "US", "GB", "CA", "AU", "SG"};
        return countries[(int)(Math.random() * countries.length)];
    }

    private String randomCity() {
        String[] cities = {"Mumbai", "Delhi", "Bangalore", "London", "New York", "Singapore"};
        return cities[(int)(Math.random() * cities.length)];
    }

    private String randomPosition() {
        String[] positions = {"pre-roll", "mid-roll", "post-roll", "banner"};
        return positions[(int)(Math.random() * positions.length)];
    }

    public static void main(String[] args) {
        new AvroAdImpressionProducer().start();
    }
}