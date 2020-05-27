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

    // =============== config options

    public static final ConfigOption<Integer> configGranularity = ConfigOptions
            .key("window.granularity")
            .intType()
            .defaultValue(15)
            .withDescription("Window granularity, in minutes. E.G for aggregation on quarters, will be 15.");

    public static final ConfigOption<Integer> configLateness = ConfigOptions
            .key("window.allowed_lateness")
            .intType()
            .defaultValue(5)
            .withDescription("How long to keep a window in memory after its time slot has passed, in minutes.");

    public static final ConfigOption<Integer> configTimeout = ConfigOptions
            .key("window.timeout")
            .intType()
            .defaultValue(10)
            .withDescription("How long a window can stay idle (no new measure) in minutes. Should be greater than window.granularity.");

    public static final ConfigOption<List<String>> configEntryPoints = ConfigOptions
            .key("cassandra.entryPoints")
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
                    "%s (%d) should be > 0", configLateness.key(), lateness));
        return Time.minutes(lateness).toMilliseconds();
    }

    public static long readTimeout(Configuration config) {
        int timeout = config.get(configTimeout);
        if (timeout < config.get(configGranularity) + config.get(configLateness))
            throw new RuntimeException(String.format("%s should be > %s + %s",
                    configTimeout.key(), configGranularity.key(), configLateness.key()));
        return Time.minutes(timeout).toMilliseconds();
    }


    public static List<String> readCassandraEntrypoints(Configuration config) {
        List<String> entrypoints = config.get(configEntryPoints);
        if (entrypoints.size() == 0)
            throw new RuntimeException(String.format(
                    "Missing Cassandra entrypoing '%s' in config.", configEntryPoints.key()));
        return entrypoints;
    }
}
