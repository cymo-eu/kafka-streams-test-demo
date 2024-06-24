package eu.cymo.kafka_streams_demo.adapter.kafka.topology;

import java.util.Properties;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TopologyConfig;
import org.apache.kafka.streams.TopologyTestDriver;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.ssl.SslBundles;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.KafkaStreamsInfrastructureCustomizer;

@Configuration
@EnableConfigurationProperties({ KafkaProperties.class })
public class TopologyTestDriverConfiguration {
    
    @Bean
    StreamsBuilder streamsBuilder(
            ObjectProvider<SslBundles> sslBundles,
            KafkaProperties kafkaProperties) {
        var streamsProperties = kafkaProperties.buildStreamsProperties(sslBundles.getIfAvailable());
        var streamsConfig = new StreamsConfig(streamsProperties);
        var topologyConfig = new TopologyConfig(streamsConfig);
        return new StreamsBuilder(topologyConfig);
    }
    
    @Bean
    TopologyTestDriver defaultTopologyTestDriver(
            ObjectProvider<KafkaStreamsInfrastructureCustomizer> infrastructureCustomizerProvider,
            ObjectProvider<SslBundles> sslBundles,
            KafkaProperties kafkaProperties,
            StreamsBuilder streamsBuilder) {
        var streamsProperties = kafkaProperties.buildStreamsProperties(sslBundles.getIfAvailable());
        var iterator = infrastructureCustomizerProvider.iterator();
        while (iterator.hasNext()) {
            iterator.next().configureBuilder(streamsBuilder);
        }
        var topology = streamsBuilder.build();
        iterator = infrastructureCustomizerProvider.iterator();
        while (iterator.hasNext()) {
            iterator.next().configureTopology(topology);
        }
        var properties = new Properties();
        properties.putAll(streamsProperties);
        return new TopologyTestDriver(topology, properties);
    }

}
