package kr.co.iwaz;

import kr.co.iwaz.kafka.KafkaConsumerEZ;
import kr.co.iwaz.kafka.KafkaProducerEZ;
import kr.co.iwaz.kafka.OffsetReset;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

public class SyncKafka extends Thread {

    private final KafkaProducerEZ producer;
    private final KafkaConsumerEZ consumer;
    private final WebSocketIdParser webSocketIdParser;
    private final Map<String, TimedFuture> futureMap = new ConcurrentHashMap<>();
    private final int timeoutSec;

    public SyncKafka(String kafkaIp, int kafkaPort, String receiveTopic, int timeoutSec,
                     WebSocketIdParser webSocketIdParser) {
        this.producer = new KafkaProducerEZ(kafkaIp, kafkaPort);
        this.consumer = new KafkaConsumerEZ
                .Builder(kafkaIp, kafkaPort)
                .topics(receiveTopic)
                .groupName("test")
                .sizeBatchReceive(1)
                .pollWaitingMS(1000)
                .autoOffsetReset(OffsetReset.LATEST)
                .build();

        this.timeoutSec = timeoutSec;
        this.webSocketIdParser = webSocketIdParser;
    }

    public CompletableFuture<String> send(String topic, String data) {
        if (!this.producer.send(topic, data)) {
            throw new RuntimeException("Error occurred while sending message");
        }

        CompletableFuture<String> future = new CompletableFuture<>();
        String websocketId = this.webSocketIdParser.parse(data);
        this.futureMap.put(websocketId, new TimedFuture(future));
        return future;
    }

    @Override
    public void run() {
        while (true) {
            for (ConsumerRecord<String, String> record : this.consumer.getRecords()) {
                try {
                    String jsonMsg = record.value();
                    String websocketId = this.webSocketIdParser.parse(jsonMsg);

                    TimedFuture timedFuture = this.futureMap.remove(websocketId);
                    if (timedFuture == null) {
                        System.out.println("타임아웃된 응답 : " + websocketId);
                    } else {
                        timedFuture.future.complete(jsonMsg);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            // 타임 아웃된 세션 처리
            Instant now = Instant.now();
            for (Map.Entry<String, TimedFuture> entry : this.futureMap.entrySet()) {
                if (now.isAfter(entry.getValue().creationTime.plusSeconds(this.timeoutSec))) {
                    CompletableFuture<String> future = this.futureMap.remove(entry.getKey()).future;
                    future.complete("timeout");
                }
            }
        }
    }

    private static class TimedFuture {
        final CompletableFuture<String> future;
        final Instant creationTime;

        public TimedFuture(CompletableFuture<String> future) {
            this.future = future;
            this.creationTime = Instant.now();
        }
    }

}

interface WebSocketIdParser {
    String parse(String data);
}
