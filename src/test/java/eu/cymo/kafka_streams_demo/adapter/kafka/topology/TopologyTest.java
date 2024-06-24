package eu.cymo.kafka_streams_demo.adapter.kafka.topology;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.autoconfigure.ImportAutoConfiguration;
import org.springframework.boot.test.autoconfigure.filter.TypeExcludeFilters;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.BootstrapWith;
import org.springframework.test.context.junit.jupiter.SpringExtension;

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Inherited
@BootstrapWith(TopologyTestContextBootstrapper.class)
@ExtendWith(SpringExtension.class)
@TypeExcludeFilters(TopologyTypeExcludeFilter.class)
@Import(value = { TopologyTestDriverConfiguration.class, MockAvroSerdeFactory.class })
@ActiveProfiles(value = { "test" })
@ImportAutoConfiguration
public @interface TopologyTest {

    ComponentScan.Filter[] includeFilters() default {};
    
    ComponentScan.Filter[] excludeFilters() default {};

    String[] properties() default {};

}
