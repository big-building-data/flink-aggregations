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

    public static long[] readWindowParameters(Configuration config) {
        long granularity = Time.minutes(config.get(configGranularity)).toMilliseconds();
        long allowedLateness = Time.minutes(config.get(configLateness)).toMilliseconds();
        if (allowedLateness < granularity) {
            throw new RuntimeException(String.format(
                    "window.allowed_lateness (%d) should be >= window.granularity (%d)", allowedLateness, granularity));
        }
        return new long[]{granularity, allowedLateness};
    }


    public static long readTimeout(Configuration config) {
        int timeout = config.get(configTimeout);
        if (timeout <= 0)
            throw new RuntimeException("window.timeout should be positive");
        return Time.minutes(timeout).toMilliseconds();
    }


    public static List<String> readCassandraEntrypoints(Configuration config) {
        return config.get(configEntryPoints);
    }
}
