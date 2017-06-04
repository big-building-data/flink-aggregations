package ch.derlin.bbdata.flink;

import ch.derlin.bbdata.commons.GsonProvider;
import ch.derlin.bbdata.flink.pojo.Measure;
import com.google.gson.Gson;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.joda.time.DateTime;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * This is just for testing: it will produce measure via a kafka producer.
 * date: 19/12/16
 *
 * @author "Lucy Linder"
 */
public class KafkaProd {
    private static final String KAFKA_INPUT_TOPIC = "input-test";


    public static void main(String[] args) throws InterruptedException, ExecutionException, IOException {
        Path configPath = Paths.get("/Users/Lin/git/BBDATA2/test/flink-custom-window-test/local.properties");
        ParameterTool parameters = ParameterTool.fromPropertiesFile(configPath.toAbsolutePath().toString());

        String topic = parameters.get("kafka.augmentation");
        Properties props = new Properties();
        //props.put( "bootstrap.servers", "localhost:9092" );
        props.put("bootstrap.servers", parameters.get("kafka.brokers"));
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(props);

        Gson gson = GsonProvider.getBuilder().serializeSpecialFloatingPointValues().create();
        DateTime dt = new DateTime().minusDays(1).plusMinutes(400);

        DateTime dt1 = new DateTime().minusDays(16);
        DateTime dt2 = new DateTime().minusDays(6);

        for (int i = 0; i < 5; i++) {
            Measure m = new Measure();
            m.objectId = i;
            m.unitSymbol = "V";
            m.type = "float";
            m.timestamp = dt1.toDate();
            dt1 = dt1.plusSeconds(30);
            m.value = "1.0";
            String s = gson.toJson(m);
            System.out.println(s);
            producer.send(new ProducerRecord<>(topic, null, s)).get();

        }//end for

        for (int i = 0; i < 10; i++) {
            Measure m = new Measure();
            m.objectId = 6;
            m.unitSymbol = "V";
            m.type = "float";
            m.value = "1.0";
            m.timestamp = dt2.toDate();
            dt2 = dt2.plusSeconds(30);
            String s = gson.toJson(m);
            System.out.println(s);
            producer.send(new ProducerRecord<>(topic, null, s)).get();

            Thread.sleep(1000);

        }//end for



        /*
        for( int i = 0; i < 20; i++ ){
            Measure measure = new Measure( true );
            measure.type = "float";
            measure.timestamp = dt.toDate();
            String s = gson.toJson( measure );
            System.out.println(s);
            measure.timestamp = new DateTime(measure.timestamp).minusSeconds( i ).toDate();
            producer.send( new ProducerRecord<>( KAFKA_INPUT_TOPIC, null, s ) ).get();
            System.out.println(measure);
            Thread.sleep( 100 );

        }    */

        producer.close();
    }//end main
}
