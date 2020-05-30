package ch.derlin.bbdata.flink;

import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.junit.jupiter.api.Assertions.*;

/**
 * date: 29.05.20
 *
 * @author Lucy Linder <lucy.derlin@gmail.com>
 */
public class TestConfigs {

    @Test
    public void testMandatory() {
        Configuration config = new Configuration();

        // mandatory window options
        assertThrows(Exception.class, () -> Configs.readCassandraEntrypoints(config));
        assertThrows(Exception.class, () -> Configs.readGranularity(config));

        // mandatory cassandra entrypoint
        assertThrows(Exception.class, () -> Configs.readCassandraEntrypoints(config));
    }

    @Test
    public void testDefault() {
        Configuration config = new Configuration();

        // flink
        assertDoesNotThrow(() -> Configs.readCheckpointsPath(config));
        assertDoesNotThrow(() -> Configs.readCheckpointsInterval(config));

        // window
        assertDoesNotThrow(() -> config.get(Configs.configTimeout));
        assertDoesNotThrow(() -> Configs.readFlushEvery(config));
    }

    @ParameterizedTest
    @CsvSource({"-1,false", "0,false", "1,true", "15,true"})
    public void testGranularity(int value, boolean ok) {
        Configuration config = new Configuration();
        config.set(Configs.configGranularity, value);

        if (!ok) {
            assertThrows(Exception.class, () -> Configs.readGranularity(config));
        } else {
            assertEquals(value * 60_000L, Configs.readGranularity(config));
        }
    }

    @ParameterizedTest
    @CsvSource({"-1,false", "0,true", "15,true"})
    public void testAllowedLateness(int value, boolean ok) {
        Configuration config = new Configuration();
        config.set(Configs.configLateness, value);

        if (!ok) {
            assertThrows(Exception.class, () -> Configs.readAllowedLateness(config));
        } else {
            assertEquals(value * 60_000L, Configs.readAllowedLateness(config));
        }
    }

    @ParameterizedTest
    @CsvSource({"-1,false", "0,true", "15,true"})
    public void testFlushEvery(int value, boolean ok) {
        Configuration config = new Configuration();
        config.set(Configs.configFlushEvery, value);

        if (!ok) {
            assertThrows(Exception.class, () -> Configs.readFlushEvery(config));
        } else {
            assertEquals(value * 60_000L, Configs.readFlushEvery(config));
        }
    }


    @ParameterizedTest
    @CsvSource({"-1,false", "0,false", "15,false", "20,true", ",true"})
    public void testTimeout(Integer value, boolean ok) {
        Configuration config = new Configuration();
        // timeout >= granularity + lateness, hence 20
        config.set(Configs.configGranularity, 15);
        config.set(Configs.configLateness, 5);

        if (value != null) config.set(Configs.configTimeout, value);

        if (!ok) {
            assertThrows(Exception.class, () -> Configs.readTimeout(config));
        } else if (value != null) {
            assertEquals(value * 60_000L, Configs.readTimeout(config));
        } else {
            assertDoesNotThrow(() -> Configs.readTimeout(config));
        }
    }

    @Test
    public void testCassandraEntrypoints(){
        Configuration config = new Configuration();
        assertThrows(Exception.class, () -> Configs.readCassandraEntrypoints(config));

        config.setString(Configs.configEntrypoints.key(), "");
        assertThrows(Exception.class, () -> Configs.readCassandraEntrypoints(config));

        config.setString(Configs.configEntrypoints.key(), "127.0.0.1");
        assertDoesNotThrow(() -> Configs.readCassandraEntrypoints(config));
        assertEquals(1, Configs.readCassandraEntrypoints(config).size());

        config.setString(Configs.configEntrypoints.key(), "127.0.0.1;localhost");
        assertDoesNotThrow(() -> Configs.readCassandraEntrypoints(config));
        assertEquals(2, Configs.readCassandraEntrypoints(config).size());

        config.setString(Configs.configEntrypoints.key(), "127.0.0.1;localhost;");
        assertDoesNotThrow(() -> Configs.readCassandraEntrypoints(config));
        assertEquals(2, Configs.readCassandraEntrypoints(config).size());

    }
}
