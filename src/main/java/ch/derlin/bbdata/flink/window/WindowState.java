package ch.derlin.bbdata.flink.window;

import ch.derlin.bbdata.flink.AggregationConfiguration;
import ch.derlin.bbdata.flink.accumulators.IAccumulator;
import ch.derlin.bbdata.flink.pojo.Measure;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashSet;
import java.util.Set;
import java.util.TreeMap;

/**
 * date: 06.03.17
 *
 * @author Lucy Linder <lucy.derlin@gmail.com>
 */
public class WindowState {

    // if not transcient, will be restored with the state and we end up with the
    // internal logger (LOG.logger) as null --> nullpointerexception upon logging
    private transient Logger LOG = LoggerFactory.getLogger(WindowState.class);

    public TreeMap<Long, IAccumulator> map = new TreeMap<>();
    public long timeAdvance, lastCleanup;

    private long windowSizeMillis, allowedLateness;
    public transient int objectId = -1;

    public WindowState() {

    }

    public WindowState(long windowSizeMillis, long allowedLateness) {
        this.windowSizeMillis = windowSizeMillis;
        this.allowedLateness = allowedLateness;
    }


    public void process(Collector<IAccumulator> collector, Measure measure) {
        long key = AggregationConfiguration.getWindowStartFor(measure, windowSizeMillis);
        long ts = measure.timestamp.getTime();
        objectId = measure.objectId;

        // update time
        if (timeAdvance == 0) {
            // first record
            timeAdvance = lastCleanup = ts;
        } else if (ts < timeAdvance - allowedLateness) {
            // late record  TODO
            if (!map.containsKey(key)) {
                LOG.warn("very late record: currentTime={}, record '{}'", timeAdvance, measure);
                // throw a new record only if the window is not still in cache
                collector.collect(AggregationConfiguration.getLateAccumulatorFor(measure, key, windowSizeMillis));
            } else {
                LOG.trace("late record '{}', but window still in scope", timeAdvance, measure);
            }
        } else {
            if (ts > timeAdvance) timeAdvance = key;
        }

        // accumulate
        IAccumulator acc;
        if (map.containsKey(key)) {
            acc = map.get(key);
        } else {
            // create a new window
            acc = AggregationConfiguration.getAccumulatorFor(measure, key, windowSizeMillis);
            map.put(key, acc);
            //System.out.printf("%s: create window ## %d/%s [%d]%n", this, measure.objectId, new Date(key), acc.accId);
        }

        acc.fold(measure);

        // cleanup
        if (timeAdvance - lastCleanup > windowSizeMillis) {
            finalizeOldWindows(collector);
            lastCleanup = timeAdvance;
        }
    }


    private void finalizeOldWindows(Collector<IAccumulator> collector) {

        Set<Long> keys = new LinkedHashSet<>(map.keySet());
        long timeout = windowSizeMillis + allowedLateness;

        for (Long key : keys) {
            if (key < timeAdvance - timeout) {
                IAccumulator acc = map.get(key);
                acc.finalise();
                collector.collect(acc);
                map.remove(key);
                LOG.trace("finalized window '{}'", acc);
            }
        }//end for
        LOG.trace("cleanup: {}", this);
    }

    public void flush(Collector<IAccumulator> collector) {
        for (IAccumulator accumulator : map.values()) {
            collector.collect(accumulator);
            LOG.trace("cleanup window '{}'", accumulator);
        }//end for
        map.clear();
    }

    @Override
    public String toString() {
        return String.format("WindowsState{objectId=%d,windows=%d}", objectId, map.size());
    }
}
