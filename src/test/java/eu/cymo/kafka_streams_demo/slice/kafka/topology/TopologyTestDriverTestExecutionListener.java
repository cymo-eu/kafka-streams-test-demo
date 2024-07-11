package eu.cymo.kafka_streams_demo.slice.kafka.topology;

import java.util.Properties;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TopologyTestDriver;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.ssl.SslBundles;
import org.springframework.context.ApplicationContext;
import org.springframework.kafka.config.KafkaStreamsInfrastructureCustomizer;
import org.springframework.test.context.TestContext;
import org.springframework.test.context.TestExecutionListener;

public class TopologyTestDriverTestExecutionListener implements TestExecutionListener {
	public static final String TOPOLOGY_TEST_DRIVER = "topologyTestDriver";

	@SuppressWarnings("resource")
	@Override
	public void beforeTestMethod(TestContext testContext) throws Exception {
		var applicationContext = testContext.getApplicationContext();
		
		var driver = testContext.computeAttribute(TOPOLOGY_TEST_DRIVER, ttd -> createTestDriver(applicationContext));
		
		for(var field : testContext.getTestClass().getDeclaredFields()) {
			if(field.getType() == TopologyTestDriver.class) {
				field.setAccessible(true);
				field.set(testContext.getTestInstance(), driver);
			}
		}
	}
	
	@Override
	public void afterTestMethod(TestContext testContext) throws Exception {
		var driver = (TopologyTestDriver) testContext.getAttribute(TOPOLOGY_TEST_DRIVER);
		
		try {
			driver.close();
		}
		catch(Exception e) {}
		
		testContext.removeAttribute(TOPOLOGY_TEST_DRIVER);
	}
	
	private TopologyTestDriver createTestDriver(ApplicationContext applicationContext) {
		var kafkaProperties = applicationContext.getBean(KafkaProperties.class);
		var sslBundles = applicationContext.getBeanProvider(SslBundles.class);
		var infrastructureCustomizer = applicationContext.getBeanProvider(KafkaStreamsInfrastructureCustomizer.class);
		var streamsBuilder = applicationContext.getBean(StreamsBuilder.class);
		
		infrastructureCustomizer.forEach(cust -> cust.configureBuilder(streamsBuilder));

		var topology = streamsBuilder.build();
		infrastructureCustomizer.forEach(cust -> cust.configureTopology(topology));
		
		var properties = new Properties();
		properties.putAll(kafkaProperties.buildStreamsProperties(sslBundles.getIfAvailable()));
		return new TopologyTestDriver(topology, properties);
	}
}
