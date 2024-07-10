package eu.cymo.kafka_streams_demo.slice.kafka.container;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.junit.jupiter.api.extension.TestInstancePostProcessor;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.ssl.SslBundles;

import eu.cymo.kafka_streams_demo.utils.Contexts;
import eu.cymo.kafka_streams_demo.utils.FieldParameter;
import eu.cymo.kafka_streams_demo.utils.ParameterContextParameter;
import eu.cymo.kafka_streams_demo.utils.ParameterSerdes;
import eu.cymo.kafka_streams_demo.utils.TestParameter;

public class ConsumerExtension implements ParameterResolver, TestInstancePostProcessor, AfterEachCallback {
	private static final String CONSUMERS = "consumers";

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
		return (parameter.isType(Consumer.class) || parameter.isType(ConsumerAssert.class)) &&
				parameter.hasAnnotation(TestConsumer.class);
	}
	
	private Object resolveParameter(
			TestParameter parameter,
			ExtensionContext extensionContext) {
		if(parameter.isType(Consumer.class)) {
			return createConsumer(parameter, extensionContext);
		}
		if(parameter.isType(ConsumerAssert.class)) {
			return new ConsumerAssert<>(createConsumer(parameter, extensionContext));
		}
		return null;
	}
	
	private Consumer<?, ?> createConsumer(
			TestParameter parameter,
			ExtensionContext extensionContext) {
		var consumer = new KafkaConsumer<>(
				getProperties(parameter, extensionContext),
				ParameterSerdes.getSerde(extensionContext, parameter, true).deserializer(),
				ParameterSerdes.getSerde(extensionContext, parameter, false).deserializer());
		consumer.subscribe(Collections.singletonList(parameter.getAnnotation(TestConsumer.class).topic()));
		
		var consumers = Contexts.<List<Consumer<?,?>>>getStoreValue(extensionContext, CONSUMERS)
			.orElseGet(ArrayList::new);
		consumers.add(consumer);
		Contexts.putStoreValue(extensionContext, CONSUMERS, consumers);
		
		return consumer;
	}
	
	private Map<String, Object> getProperties(
			TestParameter parameter,
			ExtensionContext extensionContext) {
		var properties = getKafkaProperties(extensionContext).buildConsumerProperties(getSslBundles(extensionContext));
		var groupId = parameter.getAnnotation(TestConsumer.class).groupId();
		if(!groupId.isBlank()) {
			properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		}
		properties.putAll(properties(parameter));
		return properties;
	}
	
	private Map<String, Object> properties(TestParameter parameter) {
		return Stream.of(parameter.getAnnotation(TestConsumer.class).properties())
				.map(s -> s.split("="))
				.collect(Collectors.toMap(s -> s[0], s -> s[1]));
				
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
		Contexts.<List<Consumer<?,?>>>getStoreValue(extensionContext, CONSUMERS)
			.orElseGet(ArrayList::new)
			.forEach(Consumer::close);
		Contexts.clearStoreValue(extensionContext, CONSUMERS);
	}

}
