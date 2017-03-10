package ch.derlin.bbdata.flink;


import ch.derlin.bbdata.commons.dateutils.TimeZoneUtils;
import ch.derlin.bbdata.flink.mappers.StringToFloatMeasureFlatMapper;
import ch.derlin.bbdata.flink.pojo.Measure;
import ch.derlin.bbdata.flink.sinks.CassandraSink;
import ch.derlin.bbdata.flink.window.WindowMapper;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

/**
 * date: 19/12/16
 *
 * @author "Lucy Linder"
 */
public class Main {

    private static final long DEFAULT_CHECKPOINT_INTERVAL = 5000;
    private static String UID_PREFIX = "ch.derlin.bbdata.flink.custom.window.";
    // logging
    private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

    // kafka
    private static final Object KAFKA_CONSUMER_GROUP = "derlin-bbdata-custom-window-test-01";// + r.nextInt();


    public static void main(String[] args) throws Exception {

        if (args.length < 1) {
            LOGGER.error("missing property file as first argument");
            System.exit(1);
        }

        TimeZoneUtils.setDefaultToUTC();

        // get path to configuration file
        Path configPath = Paths.get(args[0]);
        LOGGER.info("Loading properties from file: {}", configPath);

        ParameterTool parameters = ParameterTool.fromPropertiesFile(configPath.toAbsolutePath().toString());

        // ensure granularity is ok
        parameters.getRequired("window.granularity");

        // kafka
        String kafkaBrokers = parameters.getRequired("kafka.brokers");
        String kafkaInput = parameters.getRequired("kafka.augmentation");

        // DataStream from Kafka
        Properties prop = new Properties();
        prop.put("group.id", KAFKA_CONSUMER_GROUP);
        prop.put("bootstrap.servers", kafkaBrokers);
        prop.put("value.serializer", StringSerializer.class.getCanonicalName());
        prop.put("auto.offset.reset", "earliest");

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<String> inputStream = env.addSource( //
                new FlinkKafkaConsumer09<>(kafkaInput, new SimpleStringSchema(), prop));

        inputStream
                .flatMap(new StringToFloatMeasureFlatMapper())
                .returns(Measure.class)
                .rescale()
                .keyBy("objectId")
                .process(new WindowMapper())
                .uid(UID_PREFIX + "WindowBasic01")
                .addSink(new CassandraSink());


        env.setParallelism(1);
        //env.getCheckpointConfig().setCheckpointInterval(parameters.getLong("checkpoint.interval", DEFAULT_CHECKPOINT_INTERVAL));
        env.getConfig().setGlobalJobParameters(parameters.getConfiguration());
        env.execute("BBData aggregation - custom windows");
    }//end main

}
