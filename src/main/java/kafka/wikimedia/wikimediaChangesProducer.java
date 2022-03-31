package kafka.wikimedia;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.EventSource;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class wikimediaChangesProducer {

    public static void main(String[] args) throws InterruptedException {

        String bootstrapServers = "192.168.15.186:9092";

        // producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // set security configs
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");  //appears as -1
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));

        // create Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // kafka-topics --bootstrap-server 192.168.15.186:9092 --create --topic wikimedia-recentchange --partitions 3 --replication-factor 1
        String topic = "wikimedia-recentchange";

        // Handle events coming from the Steam and sending for the Producer
        EventHandler eventHandler = new wikimediaChangesHandler(producer, topic);
        String url = "https://stream.wikimedia.org/v2/stream/recentchange";
        EventSource.Builder builder = new EventSource.Builder(eventHandler, URI.create(url));
        EventSource eventSource = builder.build();

        // start the producer in another thread
        eventSource.start();

        // block the program after 10 minutes
        TimeUnit.MINUTES.sleep(10);


        // consumer command: kafka-console-consumer --bootstrap-server 192.168.15.186:9092 --topic wikimedia-recentchange --from-beginning
    }
}
