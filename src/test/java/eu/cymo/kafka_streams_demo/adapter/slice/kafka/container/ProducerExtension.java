package eu.cymo.kafka_streams_demo.adapter.slice.kafka.container;

import java.util.ArrayList;
import java.util.List;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.junit.jupiter.api.extension.TestInstancePostProcessor;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.ssl.SslBundles;

import eu.cymo.kafka_streams_demo.extension.Contexts;
import eu.cymo.kafka_streams_demo.extension.FieldParameter;
import eu.cymo.kafka_streams_demo.extension.ParameterContextParameter;
import eu.cymo.kafka_streams_demo.extension.ParameterSerdes;
import eu.cymo.kafka_streams_demo.extension.TestParameter;

public class ProducerExtension implements ParameterResolver, TestInstancePostProcessor, AfterEachCallback {
	private static final String PRODUCERS = "producers";
	
	@Override
	public void postProcessTestInstance(Object testInstance, ExtensionContext context) throws Exception {
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

	private boolean supportsParameter(TestParameter parameter) {
		return parameter.isType(Producer.class);
	}
	
	private Object resolveParameter(
			TestParameter parameter,
			ExtensionContext extensionContext) {
		if(supportsParameter(parameter)) {
			var producer = new KafkaProducer<>(
					getKafkaProperties(extensionContext).buildProducerProperties(getSslBundles(extensionContext)),
					ParameterSerdes.getSerde(extensionContext, parameter, true).serializer(),
					ParameterSerdes.getSerde(extensionContext, parameter, false).serializer());

			var producers = Contexts.<List<Producer<?,?>>>getStoreValue(extensionContext, PRODUCERS)
				.orElseGet(ArrayList::new);
			producers.add(producer);
			Contexts.putStoreValue(extensionContext, PRODUCERS, producers);
			
			return producer;
		}
		return null;
	}
	
	private KafkaProperties getKafkaProperties(ExtensionContext extensionContext) {
		return Contexts.getSpringBeanFactory(extensionContext)
				.getBean(KafkaProperties.class);
	}
	
	private SslBundles getSslBundles(ExtensionContext extensionContext) {
		return Contexts.getSpringBeanFactory(extensionContext)
				.getBeanProvider(SslBundles.class)
				.getIfAvailable();
	}

	@Override
	public void afterEach(ExtensionContext extensionContext) throws Exception {
		Contexts.<List<Producer<?,?>>>getStoreValue(extensionContext, PRODUCERS)
			.orElseGet(ArrayList::new)
			.forEach(Producer::close);
		Contexts.clearStoreValue(extensionContext, PRODUCERS);
	}
}
