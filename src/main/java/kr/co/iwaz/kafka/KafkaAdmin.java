package kr.co.iwaz.kafka;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class KafkaAdmin {
    private final AdminClient client;

    public KafkaAdmin(String brokers) {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        client = AdminClient.create(props);
    }

    @Override
    protected void finalize() {
        client.close();
    }

    /**
     * 토픽을 생성한다.
     * @param topicName 생성할 토픽명
     */
    public void createTopic(String topicName, int numPartitions) {
        List<NewTopic> newTopics = new ArrayList<>();
        newTopics.add(new NewTopic(topicName, numPartitions, (short)3));
        CreateTopicsResult result = client.createTopics(newTopics);

        // 결과 오류 검사
        final Map<String, KafkaFuture<Void>> results = result.values();
        checkResult(results, "ERROR: creating topic ");
    }

    /**
     * 토픽을 삭제한다.
     * @param topicName 삭제할 토픽명
     */
    public void deleteTopic(String topicName) {
        List<String> topics = new ArrayList<>();
        topics.add(topicName);
        DeleteTopicsResult result = client.deleteTopics(topics);

        // 결과 오류 검사
        final Map<String, KafkaFuture<Void>> results = result.values();
        checkResult(results, "ERROR: deleting topic ");
    }

    /**
     * 제어 명령 결과 검사 및 출력
     * @param results 결과를 반환할 KafkaFuture 객체들
     */
    private void checkResult(final Map<String, KafkaFuture<Void>> results, String errorMsg) {
        for (final Map.Entry<String, KafkaFuture<Void>> entry : results.entrySet()) {
            try {
                entry.getValue().get(30, TimeUnit.SECONDS);
            } catch (Exception e) {
                System.err.println(errorMsg + entry.getKey());
                e.printStackTrace(System.err);
            }
        }
    }
}
