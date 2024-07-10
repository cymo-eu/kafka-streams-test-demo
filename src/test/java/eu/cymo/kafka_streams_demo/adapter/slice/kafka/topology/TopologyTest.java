package eu.cymo.kafka_streams_demo.adapter.slice.kafka.topology;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.autoconfigure.filter.TypeExcludeFilters;
import org.springframework.boot.test.context.SpringBootTestContextBootstrapper;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.BootstrapWith;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import eu.cymo.kafka_streams_demo.adapter.slice.kafka.MockAvroSerdeFactory;

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@ExtendWith({
	SpringExtension.class, TopologyTestDriverExtension.class, TestTopicExtension.class })
@BootstrapWith(SpringBootTestContextBootstrapper.class)
@TypeExcludeFilters(TopologyTypeExcludeFilter.class)
@Import({ 
    StreamsBuilderConfiguration.class, MockAvroSerdeFactory.class })
@ActiveProfiles("test")
public @interface TopologyTest {

    ComponentScan.Filter[] includeFilters() default {};
    
    ComponentScan.Filter[] excludeFilters() default {};

}
