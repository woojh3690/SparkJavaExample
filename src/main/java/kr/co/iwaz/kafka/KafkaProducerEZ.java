package kr.co.iwaz.kafka;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.config.SslConfigs;

import java.util.Properties;
import java.util.concurrent.Future;

public class KafkaProducerEZ
{
    private KafkaProducer<String, String> producer;

    public KafkaProducerEZ(String ip, int port)
    {
        construct(ip, port, "");
    }

    public KafkaProducerEZ(String ip, int port, String jksPath)
    {
        construct(ip, port, jksPath);
    }

    @Override
    protected void finalize() throws Throwable {
        producer.close();
        super.finalize();
    }

    private void construct(String ip, int port, String jksPath)
    {
        Properties prop = new Properties();
        prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, ip + ":" + port);
        prop.put(ProducerConfig.ACKS_CONFIG, "all");
        prop.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, 64000000);

        // key & value deserializer
        prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        if (!jksPath.isEmpty()) {
            // 신뢰 저장소 인증서 추가
            prop.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");
            prop.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, jksPath + "/kafka.client.truststore.jks");
            prop.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG,  "iwaz123");
        }

        // producer 생성
        producer = new KafkaProducer<>(prop);
    }

    public boolean send(String topic, String cnt)
    {
        Future<RecordMetadata> aReturn = producer.send(new ProducerRecord<>(topic, cnt));
        producer.flush();
        return aReturn.isDone();
    }

}
