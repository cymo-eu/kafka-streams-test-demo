package eu.cymo.kafka_streams_demo.utils;

import java.lang.annotation.Annotation;
import java.lang.reflect.Type;

public interface TestParameter {

	boolean isType(Class<?> cl);
	
	boolean hasAnnotation(Class<? extends Annotation> cl);
	
	<T extends Annotation> T getAnnotation(Class<T> cl);
	
	Type[] getParameterizdTypes();
	
}
