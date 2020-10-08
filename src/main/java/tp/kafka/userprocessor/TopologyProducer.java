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
    public Topology joinChatWithUserdata() {
        StreamsBuilder builder = new StreamsBuilder();
        //(1) KTable
        //TODO: create a stream from `conf.userTopic()`. Use the provided Serdes.
        //TODO: create a materialized KTable from that stream by using `the description provided in `materializesUserTable()`

        //(2) enrich chat messages
        //TODO: create a stream from `conf.filteredTopic()`. Use the provided Serdes.
        //TODO: ensure that chat messages are processed on the processor insance which contains the user data matching the userId in the chat message.
        //TODO: Join the chat message with the user data. Use the `enrich` method to create a `RichChatMessage`
        //TODO: produce the resulting stream to `conf.outputTopic()`. Use the provided serdes.

        return builder.build();
    }

    Materialized<String, User, KeyValueStore<Bytes, byte[]>> materializesUserTable() {
        return Materialized.<String, User>as(usersByIdStoreSupplier)
                    .withKeySerde(stringSerde)
                    .withValueSerde(userSerde);
    }

 
    RichChatMessage enrich(ChatMessage msg, User user) {
        return  new RichChatMessage(user, msg.getMessage());
    }

}