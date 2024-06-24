package eu.cymo.kafka_streams_demo.adapter.kafka;

import org.apache.avro.specific.SpecificRecord;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.ssl.SslBundles;
import org.springframework.stereotype.Component;

import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;

@Component
public class KafkaPropertiesAvroSerdeFactory implements AvroSerdeFactory {
    private final KafkaProperties kafkaProperties;
    private final ObjectProvider<SslBundles> sslBundles;

    public KafkaPropertiesAvroSerdeFactory(
            KafkaProperties kafkaProperties,
            ObjectProvider<SslBundles> sslBundles) {
        this.kafkaProperties = kafkaProperties;
        this.sslBundles = sslBundles;
    }

    @Override
    public GenericAvroSerde genericAvroSerde(boolean isKey) {
        var serde = new GenericAvroSerde();
        serde.configure(kafkaProperties.buildStreamsProperties(sslBundles.getIfAvailable()), isKey);
        return serde;
    }

    @Override
    public <T extends SpecificRecord> SpecificAvroSerde<T> specificAvroSerde(boolean isKey) {
        var properties = kafkaProperties.buildStreamsProperties(sslBundles.getIfAvailable());
        properties.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
        
        var serde = new SpecificAvroSerde<T>();
        serde.configure(properties, isKey);
        return serde;
    }

}
