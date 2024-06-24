package eu.cymo.kafka_streams_demo.adapter.kafka;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import eu.cymo.kafka_streams.demo.OrderCreated;

@Component
public class StreamTopology {
    private final AvroSerdeFactory avroSerdes;
    
    public StreamTopology(
            AvroSerdeFactory avroSerdes) {
        this.avroSerdes = avroSerdes;
    }

    @Autowired
    public void configure(StreamsBuilder builder) {
        builder.stream("orders",
                       Consumed.with(Serdes.String(),
                                     avroSerdes.<OrderCreated>specificAvroValueSerde()))
               .filter((_, order) -> order.getResellerId() != null)
               .groupBy((_, order) -> order.getResellerId(),
                        Grouped.with("resellers-orders-grouped",
                                     Serdes.String(),
                                     avroSerdes.specificAvroValueSerde()))
               .count(Materialized.<String, Long, KeyValueStore<Bytes,byte[]>>as("resellers-orders-count")
                                  .withKeySerde(Serdes.String())
                                  .withValueSerde(Serdes.Long()))
               .toStream()
               .to("reseller_orders_count",
                   Produced.with(Serdes.String(),
                                 Serdes.Long()));
    }
    
}
