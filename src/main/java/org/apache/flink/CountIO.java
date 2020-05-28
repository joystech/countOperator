package org.apache.flink;


import java.nio.charset.StandardCharsets;
import java.util.Objects;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.flink.count.CountRequest;
import org.apache.flink.count.CountResponse;
import org.apache.flink.statefun.sdk.io.EgressIdentifier;
import org.apache.flink.statefun.sdk.io.EgressSpec;
import org.apache.flink.statefun.sdk.io.IngressIdentifier;
import org.apache.flink.statefun.sdk.io.IngressSpec;
import org.apache.flink.statefun.sdk.kafka.KafkaEgressBuilder;
import org.apache.flink.statefun.sdk.kafka.KafkaEgressSerializer;
import org.apache.flink.statefun.sdk.kafka.KafkaIngressBuilder;
import org.apache.flink.statefun.sdk.kafka.KafkaIngressDeserializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;


final class CountIO {

    static final IngressIdentifier<CountRequest> GREETING_INGRESS_ID =
            new IngressIdentifier<>(CountRequest.class, "apache", "greet-ingress");

    static final EgressIdentifier<CountResponse> GREETING_EGRESS_ID =
            new EgressIdentifier<>("apache", "kafka-greeting-output", CountResponse.class);

    private final String kafkaAddress;

    CountIO(String kafkaAddress) {
        this.kafkaAddress = Objects.requireNonNull(kafkaAddress);
    }

    IngressSpec<CountRequest> getIngressSpec() {
        return KafkaIngressBuilder.forIdentifier(GREETING_INGRESS_ID)
                .withKafkaAddress(kafkaAddress)
                .withTopic("input")
                .withDeserializer(CountKafkaDeserializer.class)
                .withProperty(ConsumerConfig.GROUP_ID_CONFIG, "greetings")
                .build();
    }

    EgressSpec<CountResponse> getEgressSpec() {
        return KafkaEgressBuilder.forIdentifier(GREETING_EGRESS_ID)
                .withKafkaAddress(kafkaAddress)
                .withSerializer(CountKafkaSerializer.class)
                .build();
    }

    private static final class CountKafkaDeserializer
            implements KafkaIngressDeserializer<CountRequest> {

        private static final long serialVersionUID = 1L;

        @Override
        public CountRequest deserialize(ConsumerRecord<byte[], byte[]> input) {
            String id = new String(input.value(), StandardCharsets.UTF_8);
            String name = id.split(";")[0];
            String money = id.split(";")[1];
            return CountRequest.newBuilder().setId(name).setMoney(Integer.valueOf(money)).build();
        }
    }

    private static final class CountKafkaSerializer implements KafkaEgressSerializer<CountResponse> {

        private static final long serialVersionUID = 1L;

        @Override
        public ProducerRecord<byte[], byte[]> serialize(CountResponse response) {
            byte[] key = response.getId().getBytes(StandardCharsets.UTF_8);
            byte[] value = response.getResult().getBytes(StandardCharsets.UTF_8);

            return new ProducerRecord<>("result", key, value);
        }
    }

}