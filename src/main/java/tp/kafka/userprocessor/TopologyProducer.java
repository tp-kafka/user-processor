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
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;

import io.quarkus.kafka.client.serialization.JsonbSerde;
import lombok.extern.java.Log;
import org.apache.kafka.common.serialization.Serdes.StringSerde;
import org.apache.kafka.common.serialization.Serdes.VoidSerde;
import org.apache.kafka.common.utils.Bytes;

@ApplicationScoped
@Log
public class TopologyProducer {

    private final StringSerde stringSerde = new StringSerde();
    private final JsonbSerde<User> userSerde = new JsonbSerde<>(User.class);
    private final JsonbSerde<ChatMessage> msgSerde = new JsonbSerde<>(ChatMessage.class);
    private final JsonbSerde<RichChatMessage> richMsgSerde = new JsonbSerde<>(RichChatMessage.class);


    KeyValueBytesStoreSupplier usersByIdStoreSupplier = Stores.inMemoryKeyValueStore("usersById");

    @Inject
    Configuration conf;

    @Produces
    public Topology windowedMsgCountPerUser() {
        StreamsBuilder builder = new StreamsBuilder();
        var userTable = builder.stream(conf.userTopic(), Consumed.with(stringSerde, userSerde))
            .peek(this::logUser)
            .toTable(materializesUserTable());

        builder.stream(conf.filteredTopic(), Consumed.with(stringSerde, msgSerde))
               .selectKey(this::selectUserId, Named.as("PartitionChatByUserId"))
               .join(userTable, this::enrich) 
               .peek(this::logMessage)
               .to(conf.outputTopic(), Produced.with(stringSerde, richMsgSerde));

        return builder.build();
    }

    Materialized<String, User, KeyValueStore<Bytes, byte[]>> materializesUserTable() {
        return Materialized.<String, User>as(usersByIdStoreSupplier)
                    .withKeySerde(stringSerde)
                    .withValueSerde(userSerde);
    }

    String selectUserId(String nothing, ChatMessage msg){
        return msg.getUserId();
    }

    void logUser(String key, User user){
        TopologyProducer.log.info("processing [" + key + "] -> " + user);
    }

    void logMessage(String key, RichChatMessage msg){
        TopologyProducer.log.info("enriching [" + key + "] ->" + msg);
    }

    RichChatMessage enrich(ChatMessage msg, User user) {
        return  new RichChatMessage(user, msg.getMessage());
    }

}