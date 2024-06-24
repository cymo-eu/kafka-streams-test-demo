package eu.cymo.kafka_streams_demo.adapter.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;
import static org.springframework.context.annotation.FilterType.REGEX;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.ComponentScan;

import eu.cymo.kafka_streams.demo.OrderCreated;
import eu.cymo.kafka_streams_demo.adapter.kafka.topology.TopologyTest;

@TopologyTest(
        includeFilters = { 
                @ComponentScan.Filter(type = REGEX, pattern = { "eu.cymo.kafka_streams_demo.adapter.kafka.*" })})
class StreamTopologyTest {
    @Autowired
    private AvroSerdeFactory avroSerdes;

    @Autowired
    private TopologyTestDriver driver;

    private TestInputTopic<String, OrderCreated> orders;
    private TestOutputTopic<String, Long> resellerOrdersCount;
    
    @BeforeEach
    void setup() {
        orders = driver.createInputTopic(
                "orders",
                Serdes.String().serializer(),
                avroSerdes.<OrderCreated>specificAvroValueSerde().serializer());
        resellerOrdersCount = driver.createOutputTopic(
                "reseller_orders_count",
                Serdes.String().deserializer(),
                Serdes.Long().deserializer());
    }
    
    @Test
    void countsOrdersForResellers() {
        // given
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
        
        // when
        orders.pipeInput("key-1", order1);
        orders.pipeInput("key-2", order2);
        orders.pipeInput("key-3", order3);
        
        // then
        var result = resellerOrdersCount.readKeyValuesToMap();
        
        assertThat(result)
            .contains(entry("reseller-1", 2l),
                      entry("reseller-2", 1l));
    }
    
    @Test
    void filtersOrdersWithoutReseller() {
        // given
        var order = OrderCreated.newBuilder()
                                .setOrderId("order")
                                .setCustomerId("customer")
                                .build();

        // when
        orders.pipeInput("key", order);
        
        // then
        var result = resellerOrdersCount.readKeyValuesToMap();
        
        assertThat(result).isEmpty();
    }
}
