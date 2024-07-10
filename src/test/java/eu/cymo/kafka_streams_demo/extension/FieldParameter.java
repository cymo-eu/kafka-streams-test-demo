package eu.cymo.kafka_streams_demo.extension;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

public record FieldParameter(Field field) implements TestParameter {

	@Override
	public boolean isType(Class<?> cl) {
		return field.getType() == cl;
	}

	@Override
	public boolean hasAnnotation(Class<? extends Annotation> cl) {
		return field.isAnnotationPresent(cl);
	}

	@Override
	public <T extends Annotation> T getAnnotation(Class<T> cl) {
		return field.getAnnotation(cl);
	}

	@Override
	public Type[] getParameterizdTypes() {
		var genericType = field.getGenericType();
		if(genericType instanceof ParameterizedType parameterizedType) {
			return parameterizedType.getActualTypeArguments();
		}
		return null;
	}

}
