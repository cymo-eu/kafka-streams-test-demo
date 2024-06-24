package eu.cymo.kafka_streams_demo.adapter.kafka.topology;

import java.util.HashMap;
import java.util.Map;

import org.apache.avro.specific.SpecificRecord;
import org.springframework.stereotype.Component;

import eu.cymo.kafka_streams_demo.adapter.kafka.AvroSerdeFactory;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import io.confluent.kafka.serializers.subject.TopicRecordNameStrategy;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;

@Component
public class MockAvroSerdeFactory implements AvroSerdeFactory {
    private static final SchemaRegistryClient schemaRegistryClient = MockSchemaRegistry.getClientForScope("test-scope");

    @Override
    public GenericAvroSerde genericAvroSerde(boolean isKey) {
        var serde = new GenericAvroSerde(schemaRegistryClient);
        serde.configure(getProperties(), isKey);
        return serde;
    }

    @Override
    public <T extends SpecificRecord> SpecificAvroSerde<T> specificAvroSerde(boolean isKey) {
        var properties = new HashMap<>(getProperties());
        properties.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
        
        var serde = new SpecificAvroSerde<T>();
        serde.configure(properties, isKey);
        return serde;
    }
    
    private Map<String, Object> getProperties() {
        return Map.of(
                // property must be present, but will not be used
                KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock://schema-url",
                // automatically register schemas when writing to kafka, so that the
                // SchemaRegistryClient can be updated
                KafkaAvroSerializerConfig.AUTO_REGISTER_SCHEMAS, true,
                AbstractKafkaSchemaSerDeConfig.VALUE_SUBJECT_NAME_STRATEGY, TopicRecordNameStrategy.class);
    }

}
