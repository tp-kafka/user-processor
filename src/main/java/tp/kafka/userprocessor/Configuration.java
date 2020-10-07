package tp.kafka.userprocessor;

import javax.validation.constraints.Size;

import org.eclipse.microprofile.config.inject.ConfigProperty;

import io.quarkus.arc.config.ConfigProperties;

@ConfigProperties(prefix = "userprocessor")
public interface Configuration {

     @ConfigProperty(name="topic.in.user")
     public String userTopic();
     
     @ConfigProperty(name="topic.in.filtered")
     public String filteredTopic();
     
     @ConfigProperty(name="topic.out.chat")
     public String outputTopic();
}