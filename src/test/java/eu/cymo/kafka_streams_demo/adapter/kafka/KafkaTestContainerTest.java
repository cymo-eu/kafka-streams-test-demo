package eu.cymo.kafka_streams_demo.adapter.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;
import static org.springframework.context.annotation.FilterType.REGEX;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.ComponentScan;

import eu.cymo.kafka_streams.demo.OrderCreated;
import eu.cymo.kafka_streams_demo.slice.kafka.container.ConsumerAssert;
import eu.cymo.kafka_streams_demo.slice.kafka.container.KafkaContainerTest;
import eu.cymo.kafka_streams_demo.slice.kafka.container.TestConsumer;

@KafkaContainerTest(
        includeFilters = { 
                @ComponentScan.Filter(type = REGEX, pattern = { "eu.cymo.kafka_streams_demo.adapter.kafka.*" })})
public class KafkaTestContainerTest {

	@TestConsumer(topic = "reseller_orders_count")
	private ConsumerAssert<String, Long> consumer;
	
	private Producer<String, OrderCreated> producer;
	
	
	private String input = "orders";
	
	@Test
	void countsOrdersForResellers() {
        var order1 = OrderCreated.newBuilder()
                                 .setOrderId("order-1")
                                 .setResellerId("reseller-1")
                                 .setCustomerId("customer-1")
                                 .build();
        var order2 = OrderCreated.newBuilder()
                                 .setOrderId("order-2")
                                 .setResellerId("reseller-1")
                                 .setCustomerId("customer-2")
                                 .build();
        var order3 = OrderCreated.newBuilder()
                                 .setOrderId("order-3")
                                 .setResellerId("reseller-2")
                                 .setCustomerId("customer-3")
                                 .build();

		producer.send(new ProducerRecord<String, OrderCreated>(input, "key-1", order1));
		producer.send(new ProducerRecord<String, OrderCreated>(input, "key-2", order2));
		producer.send(new ProducerRecord<String, OrderCreated>(input, "key-3", order3));
		
		consumer.verifyAsMap(records -> {
	        assertThat(records)
	            .contains(entry("reseller-1", 2l),
	                      entry("reseller-2", 1l));
		});
	}
}
