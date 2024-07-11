package eu.cymo.kafka_streams_demo.slice.kafka.container;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Map;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.ssl.SslBundles;
import org.springframework.test.context.TestContext;
import org.springframework.test.context.TestExecutionListener;

import eu.cymo.kafka_streams_demo.adapter.kafka.AvroSerdeFactory;
import eu.cymo.kafka_streams_demo.utils.FieldSerdes;

public class ProducerTestExecutionListener implements TestExecutionListener {
	private static final String PRODUCERS = "producers";

	@Override
	public void beforeTestMethod(TestContext testContext) throws Exception {
		var producers = testContext.computeAttribute(PRODUCERS, s -> new ArrayList<Producer<?, ?>>());
		
		for(var field : testContext.getTestClass().getDeclaredFields()) {
			if(supportsField(field)) {
				field.setAccessible(true);
				var producer = resolveField(field, testContext);
				producers.add(producer);
				field.set(testContext.getTestInstance(), producer);
			}
		}
	}
	
	private boolean supportsField(Field field) {
		return field.getType() == Producer.class;
	}
	
	private Producer<?, ?> resolveField(Field field, TestContext testContext) {
		return new KafkaProducer<>(
				config(testContext),
				FieldSerdes.getSerde(field, serdeFactory(testContext), true).serializer(),
				FieldSerdes.getSerde(field, serdeFactory(testContext), false).serializer());
	}
	
	private Map<String, Object> config(TestContext testContext) {
		var applicationContext = testContext.getApplicationContext();
		
		var kafkaProperties = applicationContext.getBean(KafkaProperties.class);
		var sslBundles = applicationContext.getBeanProvider(SslBundles.class);
		
		return kafkaProperties.buildProducerProperties(sslBundles.getIfAvailable());
	}
	
	private AvroSerdeFactory serdeFactory(TestContext testContext) {
		return testContext.getApplicationContext().getBean(AvroSerdeFactory.class);
	}
	
	@Override
	public void afterTestMethod(TestContext testContext) throws Exception {
		testContext.computeAttribute(PRODUCERS, s -> new ArrayList<Producer<?, ?>>())
			.forEach(Producer::close);
		testContext.removeAttribute(PRODUCERS);
	}
}
