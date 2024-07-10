package eu.cymo.kafka_streams_demo.slice.kafka.topology;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.junit.jupiter.api.extension.TestInstancePostProcessor;

import eu.cymo.kafka_streams_demo.utils.Contexts;
import eu.cymo.kafka_streams_demo.utils.FieldParameter;
import eu.cymo.kafka_streams_demo.utils.ParameterContextParameter;
import eu.cymo.kafka_streams_demo.utils.ParameterSerdes;
import eu.cymo.kafka_streams_demo.utils.TestParameter;

public class TestTopicExtension implements ParameterResolver, TestInstancePostProcessor {

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
		return isTopic(parameter) && isAnnotatedTestTopic(parameter);
	}
	
	private Object resolveParameter(
			TestParameter parameter,
			ExtensionContext extensionContext) {
		if(isInputTopic(parameter)) {
			return createTestInputTopic(parameter, extensionContext);
		}
		if(isOutputTopic(parameter)) {
			return createTestOutputTopic(parameter, extensionContext);
		}
		return null;
	}
	
	private TestInputTopic<?, ?> createTestInputTopic(
			TestParameter parameter,
			ExtensionContext extensionContext) {
		var driver = getTopologyTestDriver(extensionContext);
		return driver.createInputTopic(
				getTopic(parameter), 
				getSerde(extensionContext, parameter, true).serializer(), 
				getSerde(extensionContext, parameter, false).serializer());
	}
	
	private TestOutputTopic<?, ?> createTestOutputTopic(
			TestParameter parameter,
			ExtensionContext extensionContext) {
		var driver = getTopologyTestDriver(extensionContext);
		return driver.createOutputTopic(
				getTopic(parameter), 
				getSerde(extensionContext, parameter, true).deserializer(), 
				getSerde(extensionContext, parameter, false).deserializer());
	}
	
	private TopologyTestDriver getTopologyTestDriver(ExtensionContext extensionContext) {
		return Contexts.<TopologyTestDriver>getStoreValue(extensionContext, TopologyTestDriverExtension.TOPOLOGY_TEST_DRIVER)
				.orElseThrow(() -> new IllegalArgumentException("TopologyTestDriver has not been instantiated yet"));
	}
	
	private String getTopic(TestParameter parameter) {
		return parameter.getAnnotation(TestTopic.class)
				.value();
	}
	
	private Serde<?> getSerde(ExtensionContext extensionContext, TestParameter parameter, boolean isKey) {
		return ParameterSerdes.getSerde(extensionContext, parameter, isKey);
	}
	
	private boolean isTopic(TestParameter parameter) {
		return isInputTopic(parameter) || isOutputTopic(parameter);
	}
	
	private boolean isInputTopic(TestParameter parameter) {
		return parameter.isType(TestInputTopic.class);
	}
	
	private boolean isOutputTopic(TestParameter parameter) {
		return parameter.isType(TestOutputTopic.class);
	}
	
	private boolean isAnnotatedTestTopic(TestParameter parameter) {
		return parameter.hasAnnotation(TestTopic.class);
	}
}
