package ch.derlin.bbdata.flink;

import ch.derlin.bbdata.flink.accumulators.Accumulator;
import ch.derlin.bbdata.flink.accumulators.IAccumulator;
import ch.derlin.bbdata.flink.accumulators.LateRecordAccumulator;
import ch.derlin.bbdata.flink.pojo.Measure;
import ch.derlin.bbdata.flink.utils.DateUtil;
import ch.derlin.bbdata.flink.utils.TestUTC;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.Date;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

/**
 * date: 29.05.20
 *
 * @author Lucy Linder <lucy.derlin@gmail.com>
 */
public class TestAggregationConfig implements TestUTC {
    private static Random random = new Random();

    @ParameterizedTest
    @CsvSource({
            "1577876400000,15,1577876400000", // round
            "1577876400010,15,1577876400000",
            "1577876401000,15,1577876400000",
            "1577877299999,15,1577876400000", // one millisecond before the next

    })
    public void testWindowStart(long ts, int granularity, long expected) {
        long granularityMillis = granularity * 60_000L;
        assertEquals(granularity, DateUtil.ms2Minutes(granularityMillis));

        long result = AggregationConfiguration.getWindowStart(ts, granularityMillis);
        assertEquals(expected, result);

        Measure m = new Measure();
        m.timestamp = new Date(ts);
        result = AggregationConfiguration.getWindowStartFor(m, granularityMillis);
        assertEquals(expected, result);
    }

    @ParameterizedTest
    @CsvSource({
            "1577836800000,2020-01,2020-01", // 2020-01-01T00:00
            "1586307553010,2020-04,2020-01", // 2020-04-08T00:59:13.010
            "1592107980000,2020-06,2020-01", // 2020-06-14T04:13:00
            "1596232800000,2020-07,2020-12", // 2020-07-31T22:00
            "2587679999000,2051-12,2051-12" // 2051-12-31T23:59:59
    })
    public void testClusteringKey(long ts, String expectedQ, String expectedD) {
        long quarters = 15 * 60_000L, hours = 60 * 60_000L, days = 24 * 60 * 60_000L;

        // <= 60 minutes will cluster on months
        assertEquals(expectedQ, AggregationConfiguration.getDateClusteringKeyFor(ts, quarters));
        assertEquals(expectedQ, AggregationConfiguration.getDateClusteringKeyFor(ts, hours));
        // > 60 minutes will cluster on 6-months, 01 or 12
        assertEquals(expectedD, AggregationConfiguration.getDateClusteringKeyFor(ts, days));
    }

    @Test
    public void testGetAccFor() {
        long windowStart = 1577876400000L;
        long granularity = 15 * 60_000L;

        IAccumulator acc;
        Measure m = new Measure();
        m.objectId = 1;
        m.timestamp = new Date(windowStart + 100);
        m.floatValue = 1.2f;

        // test random basic unit
        m.unitSymbol = AggregationConfiguration.BASIC_AGGR_UNITS.get(random.nextInt(AggregationConfiguration.BASIC_AGGR_UNITS.size()));
        acc = AggregationConfiguration.getAccumulatorFor(m, windowStart, granularity);
        assertTrue(acc instanceof Accumulator);
        assertFalse(((Accumulator) acc).isWithStdev());
        checkAcc(acc);

        // test random advanced unit
        m.unitSymbol = AggregationConfiguration.ADVANCED_AGGR_UNITS.get(random.nextInt(AggregationConfiguration.ADVANCED_AGGR_UNITS.size()));
        acc = AggregationConfiguration.getAccumulatorFor(m, windowStart, granularity);
        assertTrue(acc instanceof Accumulator);
        assertTrue(((Accumulator) acc).isWithStdev());
        checkAcc(acc);

        // test late acc (don't change the type)
        acc = AggregationConfiguration.getLateAccumulatorFor(m, windowStart, granularity);
        assertTrue(acc instanceof LateRecordAccumulator);
        checkAcc(acc);
        // also ensure it was finalized
        assertEquals(1, acc.getRecord().count);
        assertFalse(Float.isNaN(acc.getRecord().mean));
        assertFalse(Float.isNaN(acc.getRecord().std));

    }

    private static void checkAcc(IAccumulator acc) {
        assertEquals(1, acc.getRecord().objectId);
        assertNotNull(acc.getRecord().date);
        assertNotNull(acc.getRecord().timestamp);
        assertEquals(15, acc.getRecord().minutes);
    }
}
