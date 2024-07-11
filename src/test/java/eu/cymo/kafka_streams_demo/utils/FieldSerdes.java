package eu.cymo.kafka_streams_demo.utils;

import java.lang.reflect.Field;
import java.lang.reflect.Type;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

import eu.cymo.kafka_streams_demo.adapter.kafka.AvroSerdeFactory;

public class FieldSerdes {

	private FieldSerdes() {}
	
	public static Serde<?> getSerde(Field field, AvroSerdeFactory avroSerdeFactory, boolean isKey) {
		var type = isKey ? getKeyType(field) : getValueType(field);
		
		if(type == String.class) {
			return Serdes.String();
		}
		if(type == Long.class) {
			return Serdes.Long();
		}
		if(SpecificRecords.isSpecificRecord(type)) {
			return avroSerdeFactory.specificAvroSerde(isKey);
		}
		throw new IllegalArgumentException("No serdes defined for type '%s'".formatted(type));
	}

	private static Type getKeyType(Field field) {
		return Fields.getParemeterizdTypes(field)[0];
	}

	private static Type getValueType(Field field) {
		return Fields.getParemeterizdTypes(field)[1];
	}
}
