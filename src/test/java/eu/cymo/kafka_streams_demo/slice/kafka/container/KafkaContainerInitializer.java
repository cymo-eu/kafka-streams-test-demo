package eu.cymo.kafka_streams_demo.slice.kafka.container;

import java.util.Map;

import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.env.MapPropertySource;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

public class KafkaContainerInitializer implements ApplicationContextInitializer<ConfigurableApplicationContext> {

	static final KafkaContainer KAFKA_CONTAINER;
	
	static {
		KAFKA_CONTAINER = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"));
		KAFKA_CONTAINER.start();
	}
	
	@Override
	public void initialize(ConfigurableApplicationContext applicationContext) {
		applicationContext.getEnvironment().getPropertySources().addFirst(new MapPropertySource(
				"test-containers", 
				Map.of(
						"spring.kafka.bootstrap-servers", KAFKA_CONTAINER.getBootstrapServers(),
						"spring.kafka.properties.security.protocol", "PLAINTEXT")));
	}
	
}
