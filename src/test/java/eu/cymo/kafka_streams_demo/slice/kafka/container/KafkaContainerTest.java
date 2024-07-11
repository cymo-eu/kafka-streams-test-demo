package eu.cymo.kafka_streams_demo.slice.kafka.container;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.autoconfigure.filter.TypeExcludeFilters;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.BootstrapWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import eu.cymo.kafka_streams_demo.slice.kafka.MockAvroSerdeFactory;

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@ExtendWith(SpringExtension.class)
@TestExecutionListeners({
	ProducerTestExecutionListener.class, ConsumerTestExecutionListener.class })
@BootstrapWith(KafkaContainerTestContextBootstraper.class)
@TypeExcludeFilters(KafkaContainerTestExcludeFilter.class)
@ContextConfiguration(initializers = { KafkaContainerInitializer.class })
@Import({ 
	MockAvroSerdeFactory.class, TopicInitializer.class })
@ActiveProfiles("test")
public @interface KafkaContainerTest {

    ComponentScan.Filter[] includeFilters() default {};
    
    ComponentScan.Filter[] excludeFilters() default {};

	String[] properties() default {};

}
