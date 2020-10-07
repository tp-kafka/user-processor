package i3oot.qdemo;

import java.time.Duration;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;
import javax.inject.Inject;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.Stores;

import io.quarkus.kafka.client.serialization.JsonbSerde;
import lombok.extern.java.Log;

@ApplicationScoped
@Log
public class TopologyProducer {

    private final JsonbSerde<ChatMessage> chatMessageSerde = new JsonbSerde<>(ChatMessage.class);
    private final JsonbSerde<Void> voidSerde = new JsonbSerde<>(Void.class);

    
    @Inject
    Configuration conf;


    @Produces
    public Topology windowedMsgCountPerUser() {
        StreamsBuilder builder = new StreamsBuilder();
        var windowedMsgCountPerUser = builder.stream(conf.inputTopic(), Consumed.with(voidSerde, chatMessageSerde))
            .peek((k,v) -> TopologyProducer.log.info(v.toString()))
            .<String>groupBy((k,v) -> v.getUserId())
            .windowedBy(TimeWindows.of(Duration.ofSeconds(2)))
            .count();
        windowedMsgCountPerUser.toStream().to("windowedMsgCountPerUser");
        return builder.build();
    }

}