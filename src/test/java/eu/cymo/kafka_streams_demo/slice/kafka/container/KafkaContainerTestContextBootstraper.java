package eu.cymo.kafka_streams_demo.slice.kafka.container;

import org.springframework.boot.test.context.SpringBootTestContextBootstrapper;
import org.springframework.test.context.TestContextAnnotationUtils;

public class KafkaContainerTestContextBootstraper extends SpringBootTestContextBootstrapper {

	@Override
	protected String[] getProperties(Class<?> testClass) {
		var kafkaContainerTest = TestContextAnnotationUtils.findMergedAnnotation(testClass, KafkaContainerTest.class);
		return (kafkaContainerTest != null) ? kafkaContainerTest.properties() : null;
	}
	
}
