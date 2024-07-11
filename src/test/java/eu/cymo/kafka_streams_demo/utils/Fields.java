package eu.cymo.kafka_streams_demo.utils;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.function.Function;

public class Fields {

	private Fields() {}
	
	public static <T, R extends Annotation> T getAnnotationValue(Field field, Class<R> annotation, Function<R, T> property) {
		return property.apply(field.getAnnotation(annotation));
	}
	
	public static Type[] getParemeterizdTypes(Field field) {
		var genericType = field.getGenericType();
		if(genericType instanceof ParameterizedType parameterizedType) {
			return parameterizedType.getActualTypeArguments();
		}
		return null;
	}
}
