package eu.cymo.kafka_streams_demo.utils;

import java.lang.annotation.Annotation;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

import org.junit.jupiter.api.extension.ParameterContext;

public record ParameterContextParameter(ParameterContext ctx) implements TestParameter {

	@Override
	public boolean isType(Class<?> cl) {
		return ctx.getParameter().getType() == cl;
	}

	@Override
	public boolean hasAnnotation(Class<? extends Annotation> cl) {
		return ctx.isAnnotated(cl);
	}

	@Override
	public <T extends Annotation> T getAnnotation(Class<T> cl) {
		return ctx.getAnnotatedElement().getAnnotation(cl);
	}

	@Override
	public Type[] getParameterizdTypes() {
		if(ctx.getParameter().getParameterizedType() instanceof ParameterizedType parameterizedType) {
			return parameterizedType.getActualTypeArguments();
		}
		return null;
	}

}
