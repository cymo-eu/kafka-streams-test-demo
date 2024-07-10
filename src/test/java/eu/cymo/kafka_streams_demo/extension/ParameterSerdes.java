package eu.cymo.kafka_streams_demo.extension;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.junit.jupiter.api.extension.ExtensionContext;

import eu.cymo.kafka_streams_demo.adapter.kafka.AvroSerdeFactory;

public class ParameterSerdes {

	private ParameterSerdes() {}
	
	public static Serde<?> getSerde(ExtensionContext extensionContext, TestParameter parameter, boolean isKey) {
		var type = isKey ? getKeyType(parameter) : getValueType(parameter);
		
		if(type == String.class) {
			return Serdes.String();
		}
		if(type == Long.class) {
			return Serdes.Long();
		}
		if(isSpecificRecord(type)) {
			return getAvroSerdeFactory(extensionContext).specificAvroSerde(isKey);
		}
		throw new IllegalArgumentException("No serdes defined for type '%s'".formatted(type));
	}
	
	private static boolean isSpecificRecord(Type type) {
		if(type instanceof Class<?> cl) {
			return SpecificRecord.class.isAssignableFrom(cl);
		}
		if(type instanceof ParameterizedType pt) {
			return isSpecificRecord(pt.getRawType());
		}
		return false;
	}
	
	private static Type getKeyType(TestParameter parameter) {
		return getParametizedTypes(parameter)[0];
	}
	
	private static Type getValueType(TestParameter parameter) {
		return getParametizedTypes(parameter)[1];
	}
	
	private static Type[] getParametizedTypes(TestParameter parameter) {
		var types = parameter.getParameterizdTypes();
		if(types != null) {
			return types;
		}
		throw new IllegalArgumentException("Expected '%s' to have parameterized type".formatted(parameter));
	}
	
	private static AvroSerdeFactory getAvroSerdeFactory(ExtensionContext extensionContext) {
		return Contexts.getSpringBeanFactory(extensionContext)
				.getBean(AvroSerdeFactory.class);
	}
}
