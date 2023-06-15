package kr.co.iwaz.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class KafkaConsumerEZ {
    private final KafkaConsumer<String, String> consumer;
    private final int pollWaitingMS;

    public KafkaConsumerEZ(Builder builder) {
        Properties kafkaProp = new Properties();
        kafkaProp.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, builder.ip + ":" + builder.port);
        kafkaProp.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");   // auto commit
        kafkaProp.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "10000");   // 세선 타임아웃
        kafkaProp.put(ConsumerConfig.GROUP_ID_CONFIG, builder.groupName);   // topic 설정
        kafkaProp.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, builder.autoOffsetReset);
        kafkaProp.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, builder.sizeBatchReceive);
        kafkaProp.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, "64000000");

        // key & value deserializer
        kafkaProp.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaProp.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        consumer = new KafkaConsumer<>(kafkaProp);
        consumer.subscribe(builder.topics);

        // 풀링 시간 설정
        pollWaitingMS = builder.pollWaitingMS;
    }

    public ConsumerRecords<String, String> getRecords() {
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(pollWaitingMS));
        if (!records.isEmpty()) consumer.commitSync();
        return records;
    }

    @Override
    protected void finalize() throws Throwable {
        consumer.close();
        super.finalize();
    }

    /* ============================= Builder ============================= */
    public static class Builder {
        private final String ip;
        private final int port;
        private List<String> topics;
        private String groupName;
        private int sizeBatchReceive = 1;
        private int pollWaitingMS = 1000;

        // offset 정보가 존재하지 않을 경우
        // latest = 가장 마지막 offset 부터
        // earliest = 가장 처음 offset 부터
        private String autoOffsetReset = OffsetReset.LATEST;

        public Builder(String ip, int port) {
            this.ip = ip;
            this.port = port;
        }

        public Builder topics(String... topics) {
            this.topics =  new ArrayList<>(Arrays.asList(topics));
            return this;
        }

        public Builder groupName(String groupName) {
            this.groupName = groupName;
            return this;
        }

        public Builder sizeBatchReceive(int num) {
            this.sizeBatchReceive = num;
            return this;
        }

        public Builder pollWaitingMS(int num) {
            this.pollWaitingMS = num;
            return this;
        }

        public Builder autoOffsetReset(String offsetReset) {
            this.autoOffsetReset = offsetReset;
            return this;
        }

        public KafkaConsumerEZ build() {
            return new KafkaConsumerEZ(this);
        }
    }
}
