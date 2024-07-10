package eu.cymo.kafka_streams_demo.extension;

import java.util.Optional;

import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ExtensionContext.Namespace;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.test.context.junit.jupiter.SpringExtension;

public class Contexts {
	
	private Contexts() {}

	public static void putStoreValue(ExtensionContext extensionContext, String name, Object obj) {
		var store = extensionContext.getStore(Namespace.GLOBAL);
		store.put(name, obj);
	}

	@SuppressWarnings("unchecked")
	public static <T> Optional<T> getStoreValue(ExtensionContext extensionContext, String name) {
		var store = extensionContext.getStore(Namespace.GLOBAL);
		return Optional.ofNullable((T) store.get(name));
	}
	
	public static void clearStoreValue(ExtensionContext extensionContext, String name) {
		var store = extensionContext.getStore(Namespace.GLOBAL);
		store.put(name, null);
	}
	
	public static ConfigurableListableBeanFactory getSpringBeanFactory(ExtensionContext extensionContext) {
		return ((ConfigurableApplicationContext) SpringExtension.getApplicationContext(extensionContext)).getBeanFactory();
	}
}
