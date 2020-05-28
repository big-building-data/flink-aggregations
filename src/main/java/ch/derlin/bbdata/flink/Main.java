package ch.derlin.bbdata.flink;


import ch.derlin.bbdata.flink.mappers.StringToFloatMeasureFlatMapper;
import ch.derlin.bbdata.flink.pojo.Measure;
import ch.derlin.bbdata.flink.sinks.CassandraSink;
import ch.derlin.bbdata.flink.utils.DateUtil;
import ch.derlin.bbdata.flink.window.WindowMapper;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

import static org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION;

/**
 * This is the Flink application for BBData - window aggregation.
 * <p>
 * The actual window size and other parameters are declaring in a properties file.
 * Usage:
 * <code>flink run flink-aggregation-{version}-full.jar {path to the properties file}</code>
 * <p>
 * date: 19/12/16
 *
 * @author "Lucy Linder"
 */
public class Main {

    private static String UID_PREFIX = "ch.derlin.bbdata.flink.custom.window.";
    private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);


    public static void main(String[] args) throws Exception {

        if (args.length < 1) {
            LOGGER.error("missing property file as first argument");
            System.exit(1);
        }

        try {
            DateUtil.setDefaultToUTC();

            // get path to configuration file
            Path configPath = Paths.get(args[0]);
            LOGGER.info("Loading properties from file: {}", configPath);

            // read config file and convert it to a flink configuration
            ParameterTool parameters = ParameterTool.fromPropertiesFile(configPath.toAbsolutePath().toString());
            Configuration config = parameters.getConfiguration();
            LOGGER.info("properties: {}", parameters.toMap());

            // ensure properties are ok
            Configs.checkConfig(config);
            int granularityMinutes = parameters.getInt(Configs.configGranularity.key());

            // read kafka parameters
            String kafkaBrokers = parameters.getRequired("kafka.brokers");
            String kafkaInput = parameters.getRequired("kafka.augmentation");
            String consumerGroup = parameters.getRequired("kafka.consumer.group");

            // configure DataStream from Kafka
            Properties prop = new Properties();
            prop.put("group.id", consumerGroup);
            prop.put("bootstrap.servers", kafkaBrokers);
            prop.put("auto.offset.reset", "earliest");

            // setup job
            final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

            DataStream<String> inputStream = env.addSource( //
                    new FlinkKafkaConsumer010<>(kafkaInput, new SimpleStringSchema(), prop), "KafkaSource");

            inputStream
                    .flatMap(new StringToFloatMeasureFlatMapper())
                    .returns(Measure.class)
                    .rescale()
                    .keyBy("objectId")
                    .process(new WindowMapper())
                    .uid(UID_PREFIX + "WindowBasic01")
                    .addSink(new CassandraSink()).name("CassandraSink");


            // setup checkpoints
            env.enableCheckpointing(Configs.readCheckpointsInterval(config), CheckpointingMode.EXACTLY_ONCE);
            String checkpointsPath = Configs.readCheckpointsPath(config);
            if (checkpointsPath != null) {
                env.getCheckpointConfig().enableExternalizedCheckpoints(RETAIN_ON_CANCELLATION);
                // similar to setting state.checkpoints.dir: <PATH>
                env.setStateBackend((StateBackend) new FsStateBackend(checkpointsPath));
            }
            // pass configuration to the jobs and go !
            env.getConfig().setGlobalJobParameters(parameters.getConfiguration());
            env.execute(String.format("bbdata-aggregation (granularity=%d)", granularityMinutes));
        } catch (Exception e) {
            LOGGER.error("{}", e);
            throw e;
        }
    }//end main

}
