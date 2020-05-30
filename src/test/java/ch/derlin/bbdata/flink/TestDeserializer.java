package ch.derlin.bbdata.flink;

import ch.derlin.bbdata.flink.mappers.StringToFloatMeasureFlatMapper;
import ch.derlin.bbdata.flink.pojo.Measure;
import ch.derlin.bbdata.flink.utils.MockCollector;
import ch.derlin.bbdata.flink.utils.TestUTC;
import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * date: 29.05.20
 *
 * @author Lucy Linder <lucy.derlin@gmail.com>
 */
public class TestDeserializer implements TestUTC {

    private String getMeasureString(int objectId, String value, String timestamp, String unitSymbol, String type) {
        return String.format(
                "{\"objectId\":%d,\"value\":\"%s\",\"timestamp\":\"%s\",\"unitSymbol\":\"%s\",\"type\":\"%s\"," +
                        "\"comment\":null,\"unitName\":\"any\",\"owner\":1}",
                objectId, value, timestamp, unitSymbol, type);
    }

    private String getMeasureString(String value, String timestamp) {
        return getMeasureString(1, value, timestamp, "V", "float");
    }

    @ParameterizedTest
    @CsvSource({
            "1.2,2051-12-31T23:59:59Z,2587679999000",
            "-1.2,2020-01-01T00:00:00Z,1577836800000",
            "0,2020-01-01T00:00:00Z,1577836800000",
            "10,2020-01-01Z,1577836800000",
            "0.233333333333,2020-01-01T00:00Z,1577836800000",

    })
    public void testDeserializeFloats(float value, String timestamp, long expectedTs) throws Exception {
        MockCollector<Measure> collector = new MockCollector<>();
        StringToFloatMeasureFlatMapper mapper = new StringToFloatMeasureFlatMapper();
        mapper.open(new Configuration());

        assertDoesNotThrow(() -> mapper.flatMap(getMeasureString(Float.toString(value), timestamp), collector));
        assertEquals(1, collector.size());

        Measure m = collector.getLast();
        assertEquals(expectedTs, m.timestamp.getTime());
        assertEquals(value, m.floatValue);
    }

    @Test
    public void testDeserializeNonFloat() throws Exception {
        MockCollector<Measure> collector = new MockCollector<>();
        StringToFloatMeasureFlatMapper mapper = new StringToFloatMeasureFlatMapper();
        mapper.open(new Configuration());

        for (String v : new String[]{"true", "false", "NaN", "", "on", "1.2.2"})
            assertDoesNotThrow(() -> mapper.flatMap(getMeasureString(v, "2020-01-01T00:00:00Z"), collector));

        assertEquals(0, collector.size());
    }

    @Test
    public void testDeserializeWrongTimestamp() throws Exception {
        MockCollector<Measure> collector = new MockCollector<>();
        StringToFloatMeasureFlatMapper mapper = new StringToFloatMeasureFlatMapper();
        mapper.open(new Configuration());

        for (String ts : new String[]{"2020", "2020-31-12T10:00Z", "", "Z", "1900Z", "0000-00-00T00:00Z"})
            assertDoesNotThrow(() -> mapper.flatMap(getMeasureString("1.2", ts), collector));

        assertEquals(0, collector.size());
    }

}
