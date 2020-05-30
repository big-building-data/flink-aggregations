package ch.derlin.bbdata.flink;

import ch.derlin.bbdata.flink.accumulators.Accumulator;
import ch.derlin.bbdata.flink.pojo.AggregationRecord;
import ch.derlin.bbdata.flink.pojo.Measure;
import ch.derlin.bbdata.flink.utils.TestUTC;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * date: 29.05.20
 *
 * @author Lucy Linder <lucy.derlin@gmail.com>
 */
public class TestAccumulator implements TestUTC {

    private static Random random = new Random();
    private static final long START_TS = 1577876400000L, WSIZE = 15 * 60_000L;
    private static final String DATE = "2020-01";
    private static final int OID = 1;

    static class ExactAcc {
        List<Float> values;
        float min = Float.MAX_VALUE, max = 0f, sum = 0f;

        ExactAcc() {
            values = new ArrayList<>();
        }

        float gen() {
            float v = random.nextFloat() * random.nextInt(100);
            sum += v;
            min = Math.min(min, v);
            max = Math.max(max, v);
            values.add(v);
            return v;
        }

        float last() {
            return values.get(values.size() - 1);
        }

        float mean() {
            // compute true values
            return sum / values.size();
        }

        float std() {
            float sd = 0, mean = mean();
            for (float num : values) {
                sd += Math.pow(num - mean, 2);
            }
            return (float) Math.sqrt(sd / values.size());
        }

        void checkRecord(AggregationRecord acc, long ts) {
            assertEquals(values.size(), acc.count);
            assertEquals(sum, acc.sum);
            assertEquals(mean(), acc.mean);
            assertEquals(std(), acc.std, 0.001f);

            assertEquals(last(), acc.lastMeasure);
            assertEquals(ts, acc.lastMeasureTimestamp);

            assertEquals(min, acc.min);
            assertEquals(max, acc.max);
        }
    }

    private static Accumulator newAccumulator() {
        // dummy
        return new Accumulator(START_TS, WSIZE, DATE, OID);
    }


    @RepeatedTest(100)
    public void testAccRandom() {
        Measure m = new Measure();
        Accumulator acc = newAccumulator();

        long ts = START_TS;
        ExactAcc expected = new ExactAcc();

        // test fold
        int n = random.nextInt(1000) + 2;
        for (int i = 0; i < n; i++) {
            ts += random.nextInt(60000) + 100;
            m.floatValue = expected.gen();
            m.timestamp = new Date(ts);
            acc.fold(m);
        }

        acc.finalise();
        expected.checkRecord(acc.getRecord(), ts);

        // test addOne with new records
        for (int i = 0; i < 2; i++) {
            Accumulator lateAcc = newAccumulator();

            m.floatValue = expected.gen();
            ts += random.nextInt(60000) + 100;
            m.timestamp = new Date(ts);

            lateAcc.fold(m);
            lateAcc.finalise();
            acc.addOne(lateAcc.getRecord());

            expected.checkRecord(acc.getRecord(), ts);
        }

        // test addOne, with old record
        float last = expected.last();
        Accumulator lateAcc = null;
        for (int i = 0; i < 2; i++) {
            lateAcc = newAccumulator();

            m.floatValue = expected.gen();
            m.timestamp = new Date(ts - random.nextInt(60000) - 1);

            lateAcc.fold(m);
            lateAcc.finalise();
            acc.addOne(lateAcc.getRecord());

            assertEquals(last, acc.lastMeasure);
            assertEquals(ts, acc.lastMeasureTimestamp);
        }
    }

    @Test
    public void testAddOneFail() {
        Measure m = new Measure();
        Accumulator acc = newAccumulator();

        m.objectId = 1;
        m.timestamp = new Date(START_TS);
        m.floatValue = 1.2f;

        // add to empty acc

        Accumulator lateAcc1 = newAccumulator();
        lateAcc1.fold(m);
        lateAcc1.finalise();
        assertThrows(Exception.class, () -> acc.addOne(lateAcc1.getRecord()));

        //  make acc correct
        acc.fold(m);
        acc.fold(m);

        // add empty acc
        assertThrows(Exception.class, () -> acc.addOne(newAccumulator().getRecord()));

        // add wrong  cassandra keys
        AggregationRecord record = lateAcc1.getRecord();
        record.objectId = 2;
        assertThrows(Exception.class, () -> acc.addOne(record));
        record.objectId = OID;
        record.date = "2000-01";
        assertThrows(Exception.class, () -> acc.addOne(record));
        record.date = DATE;
        record.minutes = 40;
        assertThrows(Exception.class, () -> acc.addOne(record));

    }


}
