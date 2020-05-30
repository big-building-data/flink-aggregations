package ch.derlin.bbdata.flink;

import ch.derlin.bbdata.flink.accumulators.AdvancedAccumulator;
import ch.derlin.bbdata.flink.accumulators.IAccumulator;
import ch.derlin.bbdata.flink.accumulators.LateRecordAccumulator;
import ch.derlin.bbdata.flink.pojo.Measure;
import ch.derlin.bbdata.flink.utils.MockCollector;
import ch.derlin.bbdata.flink.window.WindowState;
import org.apache.flink.api.common.time.Time;
import org.junit.jupiter.api.Test;

import java.util.Date;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * date: 29.05.20
 *
 * @author Lucy Linder <lucy.derlin@gmail.com>
 */
public class TestWindowsState {

    private static final long MINUTE = Time.minutes(1).toMilliseconds();
    private static final long START_DATE = 1577876400000L; // in milliseconds

    @Test
    public void testBasicNoLateness() {

        WindowState window = new WindowState(MINUTE, 0, 0);
        MockCollector<IAccumulator> collector = new MockCollector<>();

        Measure m = new Measure();
        m.objectId = 1;
        m.unitSymbol = "V";

        for (int i = 0; i <= 16; i++) { // every 15 secs for 4 minutes (last one included for flushing)
            m.floatValue = 1.2f;
            m.timestamp = new Date(START_DATE + i * 15 * 1000);
            System.out.println(m.timestamp);
            window.process(collector, m);
        }

        assertEquals(4, collector.size());

        for (IAccumulator acc : collector.getAll()) {
            assertEquals(1.2f, acc.getRecord().mean);
            assertEquals(4, acc.getRecord().count);
            assertEquals(4 * 1.2f, acc.getRecord().sum);
            assertEquals(0f, acc.getRecord().std);
        }
    }

    @Test
    public void testBasicWithLateness() {

        WindowState window = new WindowState(2 * MINUTE, MINUTE, 0);
        MockCollector<IAccumulator> collector = new MockCollector<>();

        Measure m = new Measure();
        m.objectId = 1;
        m.unitSymbol = "V";
        m.floatValue = 1.2f;

        // open the window
        m.timestamp = new Date(START_DATE);
        window.process(collector, m);

        // one more record inside the window, advance + 1
        m.timestamp = new Date(START_DATE + MINUTE);
        window.process(collector, m);

        assertEquals(0, collector.size());

        // one new window, time advance + (3-100ms) minutes
        m.timestamp = new Date(START_DATE + (3 * MINUTE) - 100);
        window.process(collector, m);

        assertEquals(0, collector.size());

        // one late record, still [exactly] inside the allowedLateness
        m.timestamp = new Date(START_DATE + (2 * MINUTE) - 100);
        window.process(collector, m);

        assertEquals(0, collector.size());

        // time advance + (4-100ms) minutes -> will close the window
        m.timestamp = new Date(START_DATE + (4 * MINUTE) - 100);
        window.process(collector, m);

        assertEquals(1, collector.size());
        assertTrue(collector.getLast() instanceof AdvancedAccumulator);
        assertEquals(3, collector.getLast().getRecord().count); // 2 ok + 1 late

        // one late record, outside the allowedLateness
        m.timestamp = new Date(START_DATE + MINUTE - 100);
        window.process(collector, m);

        assertEquals(2, collector.size());
        assertTrue(collector.getLast() instanceof LateRecordAccumulator);
    }


    @Test
    public void testFlush() {
        WindowState window = new WindowState(MINUTE, 4 * MINUTE, 0);
        MockCollector<IAccumulator> collector = new MockCollector<>();

        Measure m = new Measure();
        m.objectId = 1;
        m.unitSymbol = "V";
        m.floatValue = 1.2f;

        for (int i = 0; i < 4; i++) {
            m.timestamp = new Date(START_DATE + i * MINUTE);
            window.process(collector, m);
        }

        assertEquals(0, collector.size());
        window.flush(collector);
        assertEquals(4, collector.size());
    }
}
