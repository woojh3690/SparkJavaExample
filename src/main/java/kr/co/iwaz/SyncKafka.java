package kr.co.iwaz;

import kr.co.iwaz.kafka.KafkaConsumerEZ;
import kr.co.iwaz.kafka.KafkaProducerEZ;
import kr.co.iwaz.kafka.OffsetReset;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

public class SyncKafka extends Thread {

    private final KafkaProducerEZ producer;
    private final KafkaConsumerEZ consumer;

    private final WebSocketIdParser webSocketIdParser;

    private final static Map<String, CompletableFuture<String>> futureMap = new ConcurrentHashMap<>();

    public SyncKafka(WebSocketIdParser webSocketIdParser) {
        this.webSocketIdParser = webSocketIdParser;

        String ip = "192.168.0.218";
        int port = 9092;

        this.producer = new KafkaProducerEZ(ip, port);
        this.consumer = new KafkaConsumerEZ
                .Builder(ip, port)
                .topics("sync")
                .groupName("test")
                .sizeBatchReceive(1)
                .pollWaitingMS(60000)
                .autoOffsetReset(OffsetReset.LATEST)
                .build();
    }

    public CompletableFuture<String> send(String topic, String data) {
        if (!producer.send(topic, data)) {
            throw new RuntimeException("Error occurred while sending message");
        }

        CompletableFuture<String> future = new CompletableFuture<>();
        String websocketId = webSocketIdParser.parse(data);
        futureMap.put(websocketId, future);
        return future;
    }

    @Override
    public void run() {
        while (true) {
            for (ConsumerRecord<String, String> record : consumer.getRecords()) {
                String jsonMsg = record.value();
                String websocketId = webSocketIdParser.parse(jsonMsg);

                CompletableFuture<String> future = futureMap.remove(websocketId);
                if (future != null) {
                    future.complete(jsonMsg);
                }
            }
        }
    }

}

interface WebSocketIdParser {
    String parse(String data);
}
