package ch.derlin.bbdata.flink;

import ch.derlin.bbdata.flink.accumulators.AdvancedAccumulator;
import ch.derlin.bbdata.flink.pojo.AggregationRecord;
import ch.derlin.bbdata.flink.pojo.Measure;
import ch.derlin.bbdata.flink.utils.TestUTC;
import org.junit.jupiter.api.RepeatedTest;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * date: 29.05.20
 *
 * @author Lucy Linder <lucy.derlin@gmail.com>
 */
public class TestAdvancedAccumulator implements TestUTC {

    private static Random random = new Random();
    private static final long START_TS = 1577876400000L;

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

    private static void initRecord(AggregationRecord acc) {
        // dummy
        acc.objectId = 1;
        acc.date = "2020-01";
        acc.minutes = 15;
    }


    @RepeatedTest(100)
    public void testAccRandom() {
        Measure m = new Measure();
        AdvancedAccumulator acc = new AdvancedAccumulator();
        initRecord(acc.getRecord());

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
            AdvancedAccumulator lateAcc = new AdvancedAccumulator();
            initRecord(lateAcc.getRecord());
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

        for (int i = 0; i < 2; i++) {
            AdvancedAccumulator lateAcc = new AdvancedAccumulator();
            initRecord(lateAcc.getRecord());
            m.floatValue = expected.gen();
            m.timestamp = new Date(ts - random.nextInt(60000) - 1);

            lateAcc.fold(m);
            lateAcc.finalise();
            acc.addOne(lateAcc.getRecord());

            assertEquals(last, acc.lastMeasure);
            assertEquals(ts, acc.lastMeasureTimestamp);
        }
    }

}
