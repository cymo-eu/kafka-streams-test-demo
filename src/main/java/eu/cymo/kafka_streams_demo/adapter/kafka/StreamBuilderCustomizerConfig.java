package eu.cymo.kafka_streams_demo.adapter.kafka;

import static org.apache.kafka.streams.KafkaStreams.State.ERROR;
import static org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.REPLACE_THREAD;
import static org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_CLIENT;

import org.springframework.boot.SpringApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.StreamsBuilderFactoryBeanConfigurer;

@Configuration
public class StreamBuilderCustomizerConfig {
    
    @Bean
    StreamsBuilderFactoryBeanConfigurer streamsBuilderFactoryBeanConfigurer(ApplicationContext ctx) {
        return factoryBean -> {
            // exception handler
            factoryBean.setStreamsUncaughtExceptionHandler(ex -> {
                if(ex instanceof RecoverableException) {
                    return REPLACE_THREAD;
                }
                return SHUTDOWN_CLIENT;
            });
            
            // state listener
            factoryBean.setStateListener((oldState, newState) -> {
                if(newState == ERROR) {
                    SpringApplication.exit(ctx);
                    System.exit(1);
                }
            });
        };
    }
    
}
