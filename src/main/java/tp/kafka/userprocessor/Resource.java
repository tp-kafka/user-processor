package tp.kafka.userprocessor;

import java.util.ArrayList;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.QueryableStoreTypes;

import lombok.extern.java.Log;

@Path("/")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
@Log
public class Resource {
    @Inject
    KafkaStreams streams;

    @Inject
    Topology topology;

    @GET
    public ArrayList<User> readLocalData(){
        var store = streams.store(
                 StoreQueryParameters.fromNameAndType("usersById", QueryableStoreTypes.<String, User>keyValueStore())
        );

        var result = new ArrayList<User>();
        store.all().forEachRemaining(kv -> result.add(kv.value));
        return result;
    }

}