package ch.derlin.bbdata.flink;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;

import java.util.List;

/**
 * date: 27.05.20
 *
 * @author Lucy Linder <lucy.derlin@gmail.com>
 */
public class Configs {

    // =============== windowing options

    public static final ConfigOption<Integer> configGranularity = ConfigOptions
            .key("window.granularity")
            .intType()
            .noDefaultValue()
            .withDescription("Window granularity, in minutes. E.G for aggregation on quarters, will be 15.");

    public static final ConfigOption<Integer> configLateness = ConfigOptions
            .key("window.allowed_lateness")
            .intType()
            .noDefaultValue()
            .withDescription("How long to keep a window in memory after its time slot has passed, in minutes.");

    public static final ConfigOption<Integer> configTimeout = ConfigOptions
            .key("window.timeout")
            .intType()
            .noDefaultValue()
            .withDescription("How long a window can stay idle (no new measure) in minutes. Should be greater than window.granularity.");

    public static final ConfigOption<Integer> configFlushEvery = ConfigOptions
            .key("window.flush_every")
            .intType()
            .defaultValue(2)
            .withDescription("Frequency of 'cleaning', that is going through the open windows and closing old ones, in minutes." +
                    "The process can take a bit, hence why we avoid doing it upon each record.");

    // =============== flink options

    public static final ConfigOption<Long> configCheckpointsInterval = ConfigOptions
            .key("flink.checkpoints.interval")
            .longType()
            .defaultValue(60000L)
            .withDescription("Flink checkpoints frequency, in milliseconds.");

    public static final ConfigOption<String> configCheckpointsPath = ConfigOptions
            .key("flink.checkpoints.path")
            .stringType()
            .noDefaultValue()
            .withDescription("Flink checkpoints path (e.g. hdfs:///user/flink, file:///user/flink). " +
                    "If not set, checkpoints won't be retained on cancellation and the path will be read from flink.conf.");

    // =============== cassandra options

    public static final ConfigOption<List<String>> configEntrypoints = ConfigOptions
            .key("cassandra.entrypoints")
            .stringType()
            .asList()
            .noDefaultValue()
            .withDescription("Comma-separated list of entrypoints (IP, host) to connect to Cassandra");

    // =============== getters

    public static long readGranularity(Configuration config) {
        long granularity = config.get(configGranularity);
        if (granularity <= 0)
            throw new RuntimeException(String.format(
                    "%s (%d) should be > 0", configGranularity.key(), granularity));
        return Time.minutes(granularity).toMilliseconds();
    }

    public static long readAllowedLateness(Configuration config) {
        long lateness = config.get(configLateness);
        if (lateness < 0)
            throw new RuntimeException(String.format(
                    "%s (%d) should be >= 0", configLateness.key(), lateness));
        return Time.minutes(lateness).toMilliseconds();
    }

    public static long readTimeout(Configuration config) {
        Integer timeout = config.get(configTimeout); // in minutes
        int minTimeout = config.get(configGranularity) + config.get(configLateness); // in minutes
        if (timeout != null) {
            if (timeout < minTimeout)
                throw new RuntimeException(String.format("%s should be > %d (%s + %s)",
                        configTimeout.key(), minTimeout, configGranularity.key(), configLateness.key()));
        } else {
            // not set: default to granularity + lateness
            timeout = minTimeout;
        }
        return Time.minutes(timeout).toMilliseconds();
    }

    public static long readFlushEvery(Configuration config) {
        int flushEvery = config.get(configFlushEvery);
        if (flushEvery <= 0)
            throw new RuntimeException(String.format(
                    "%s (%d) should be > 0", configFlushEvery.key(), flushEvery));
        return Time.minutes(flushEvery).toMilliseconds();
    }

    public static long readCheckpointsInterval(Configuration config) {
        return config.get(configCheckpointsInterval);
    }

    public static String readCheckpointsPath(Configuration config) {
        return config.get(configCheckpointsPath);
    }


    public static List<String> readCassandraEntrypoints(Configuration config) {
        List<String> entrypoints = config.get(configEntrypoints);
        if (entrypoints.size() == 0)
            throw new RuntimeException(String.format(
                    "Missing Cassandra entrypoing '%s' in config.", configEntrypoints.key()));
        return entrypoints;
    }


    public static void checkConfig(Configuration config) {
        // windows
        readGranularity(config);
        readAllowedLateness(config);
        readTimeout(config);
        readFlushEvery(config);
        // flink
        readCheckpointsInterval(config);
        readCheckpointsPath(config);
        // cassandra
        readCassandraEntrypoints(config);
    }
}
