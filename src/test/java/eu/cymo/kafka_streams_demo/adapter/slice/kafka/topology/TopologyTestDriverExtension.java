package eu.cymo.kafka_streams_demo.adapter.slice.kafka.topology;

import java.util.Properties;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.junit.jupiter.api.extension.TestInstancePostProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.ssl.SslBundles;
import org.springframework.kafka.config.KafkaStreamsInfrastructureCustomizer;

import eu.cymo.kafka_streams_demo.extension.Contexts;
import eu.cymo.kafka_streams_demo.extension.FieldParameter;
import eu.cymo.kafka_streams_demo.extension.ParameterContextParameter;
import eu.cymo.kafka_streams_demo.extension.TestParameter;

public class TopologyTestDriverExtension implements ParameterResolver, TestInstancePostProcessor, AfterEachCallback {
	private Logger log = LoggerFactory.getLogger(TopologyTestDriverExtension.class);
	private static boolean topologyLogged = false;
	
	public static final String TOPOLOGY_TEST_DRIVER = "topologyTestDriver";

	@Override
	public void postProcessTestInstance(
			Object testInstance,
			ExtensionContext context) throws Exception {
		Contexts.putStoreValue(context, TOPOLOGY_TEST_DRIVER, createTestDriver(context));

		for(var field : testInstance.getClass().getDeclaredFields()) {
			var parameter = new FieldParameter(field);
			if(supportsParameter(parameter)) {
				field.setAccessible(true);
				field.set(testInstance, resolveParameter(parameter, context));
			}
		}
	}
	
	@Override
	public boolean supportsParameter(
			ParameterContext parameterContext,
			ExtensionContext extensionContext) throws ParameterResolutionException {
		return supportsParameter(new ParameterContextParameter(parameterContext));
	}

	@Override
	public Object resolveParameter(
			ParameterContext parameterContext,
			ExtensionContext extensionContext) throws ParameterResolutionException {
		return resolveParameter(new ParameterContextParameter(parameterContext), extensionContext);
	}

	@Override
	public void afterEach(ExtensionContext extensionContext) throws Exception {
		Contexts.<TopologyTestDriver>getStoreValue(extensionContext, TOPOLOGY_TEST_DRIVER)
			.ifPresent(driver -> {
				try {
					driver.close();
				}
				catch(Exception e) {}
			});
		Contexts.clearStoreValue(extensionContext, TOPOLOGY_TEST_DRIVER);
	}
	
	private boolean supportsParameter(TestParameter parameter) {
		return parameter.isType(TopologyTestDriver.class);
	}
	
	private Object resolveParameter(TestParameter parameter, ExtensionContext extensionContext) {
		if(parameter.isType(TopologyTestDriver.class)) {
			return Contexts.getStoreValue(extensionContext, TOPOLOGY_TEST_DRIVER)
					.orElseThrow(() -> new IllegalArgumentException("TopologyTestDriver has not been instantiated yet"));
		}
		return null;
	}
	
	private TopologyTestDriver createTestDriver(ExtensionContext context) {
		var beanFactory = Contexts.getSpringBeanFactory(context);
		
		return defaultTopologyTestDriver(
				beanFactory.getBeanProvider(KafkaStreamsInfrastructureCustomizer.class),
				beanFactory.getBeanProvider(SslBundles.class),
				beanFactory.getBean(KafkaProperties.class),
				beanFactory.getBean(StreamsBuilder.class));
	}
    
    private TopologyTestDriver defaultTopologyTestDriver(
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
        if(!topologyLogged) {
        	log.info("Topology definition:\n" + topology.describe());
        	topologyLogged = true;
        }
        var properties = new Properties();
        properties.putAll(streamsProperties);
        return new TopologyTestDriver(topology, properties);
    }
	
}
