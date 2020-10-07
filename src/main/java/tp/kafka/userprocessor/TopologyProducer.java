package tp.kafka.userprocessor;

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
import org.apache.kafka.common.serialization.Serdes.StringSerde;

@ApplicationScoped
@Log
public class TopologyProducer {

    private final JsonbSerde<User> userSerde = new JsonbSerde<>(User.class);
    private final StringSerde stringSerde = new StringSerde();
    KeyValueBytesStoreSupplier usersByIdStoreSupplier = Stores.inMemoryKeyValueStore("usersById");


    @Inject
    Configuration conf;

    @Produces
    public Topology windowedMsgCountPerUser() {
        StreamsBuilder builder = new StreamsBuilder();
        var userTable = builder.stream(conf.userTopic(), Consumed.with(stringSerde, userSerde))
            .peek(this::logUser)
            .toTable(Materialized.<String, User>as(usersByIdStoreSupplier)
                .withKeySerde(stringSerde)
                .withValueSerde(userSerde)
            );
     
        return builder.build();
    }

    void logUser(String key, User user){
        TopologyProducer.log.info("processing [" + key + "] -> " + user);
    }

}