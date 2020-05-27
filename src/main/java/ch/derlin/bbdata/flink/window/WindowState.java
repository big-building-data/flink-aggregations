package ch.derlin.bbdata.flink.window;

import ch.derlin.bbdata.flink.AggregationConfiguration;
import ch.derlin.bbdata.flink.accumulators.IAccumulator;
import ch.derlin.bbdata.flink.pojo.Measure;
import ch.derlin.bbdata.flink.utils.DateUtil;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashSet;
import java.util.Set;
import java.util.TreeMap;

/**
 * A WindowState is saved by Flink {@link WindowMapper} and handles the windows.
 * It is made of a hashmap of windows {windowStart => accumulator} and processes measures from the same source
 * (i.e. with the same {@link Measure#objectId}).
 * <p>
 * Each window state keeps track of the time advance ({@link #timeAdvance}), which can vary between sources.
 * A record is considered late if its timestamp is less than {@link #timeAdvance}/{@link #allowedLateness}.
 * <p>
 * When a new measure arrives, there are three possible cases:
 * <ol>
 * <li>the measure is "on-time": update the current accumulator</li>
 * <li>the measure is late, but the window it belongs to is still in memory: update the corresponding accumulator</li>
 * <li>the measure is late, and the window has already been flushed: immediately  create and
 * forward a {@link ch.derlin.bbdata.flink.accumulators.LateRecordAccumulator}</li>
 * </ol>
 * date: 06.03.17
 *
 * @author Lucy Linder <lucy.derlin@gmail.com>
 */
public class WindowState {

    // if not transient, will be restored with the state and we end up with the
    // internal logger (LOG.logger) as null --> nullpointerexception upon logging
    private transient Logger LOG = LoggerFactory.getLogger(WindowState.class);

    /**
     * The map of windows (a time start and an accumulator)
     */
    public TreeMap<Long, IAccumulator> map = new TreeMap<>();
    /**
     * the current time for this object. Time should always process forward
     */
    public long timeAdvance;
    /**
     * the last time we flushed the windows
     */
    public long lastCleanup;

    // the size of the windows, in milliseconds
    private long windowSizeMillis;
    // the max lateness allowed to consider the record "on-time" (in ms)
    private long allowedLateness;
    // the source identification (all measure treated by this instance should have this objectId)
    public transient int objectId = -1;

    public WindowState() {
    }

    /**
     * create a window state
     *
     * @param windowSizeMillis the size of the window, in milliseconds
     * @param allowedLateness  the lateness allowed, in milliseconds
     */
    public WindowState(long windowSizeMillis, long allowedLateness) {
        this.windowSizeMillis = windowSizeMillis;
        this.allowedLateness = allowedLateness;
    }

    /**
     * Process one measure, updating the window it belongs to.
     *
     * @param collector the collector, to flush closed windows
     * @param measure   the measure to process
     */
    public void process(Collector<IAccumulator> collector, Measure measure) {
        // the window the measure belongs to (window start in milliseconds since epoch)
        long key = AggregationConfiguration.getWindowStartFor(measure, windowSizeMillis);
        // the timestamp of this measure
        long ts = measure.timestamp.getTime();
        // the objectId: update it every time to ensure it is initialized
        objectId = measure.objectId;


        if (timeAdvance == 0) {
            // this is the first record we process
            timeAdvance = lastCleanup = ts;

        } else if (ts < timeAdvance - allowedLateness) {
            // the measure is late !
            if (!map.containsKey(key)) {
                // the window it belongs has already been closed,
                // immediately forward a LateRecordAccumulator
                LOG.debug("LATE RECORD: time={}, '{}'", DateUtil.dateToString(timeAdvance), measure);
                collector.collect(AggregationConfiguration.getLateAccumulatorFor(measure, key, windowSizeMillis));
                return; // nothing left to do
            } else {
                // the window it belongs to is still in memory, so we can process it normally
                LOG.debug("LATE RECORD: (window in scope) {} '{}'", timeAdvance, measure);
            }
        } else {
            // the measure is "on-time", update the timeAdvance if necessary
            if (ts > timeAdvance) timeAdvance = key;
        }

        // accumulate
        IAccumulator acc;
        if (map.containsKey(key)) {
            // the window exists and is in memory, update it
            acc = map.get(key);
        } else {
            // there is no window yet. Create a new one.
            acc = AggregationConfiguration.getAccumulatorFor(measure, key, windowSizeMillis);
            map.put(key, acc);
        }

        // ask the accumulator to update its statistics
        acc.fold(measure);

        // every once in awhile, check for "old window" that we can close and forward.
        // don't do it every time to reduce useless processing
        if (timeAdvance - lastCleanup > windowSizeMillis) {
            finalizeOldWindows(collector);
            lastCleanup = timeAdvance;
        }
    }

    //

    /**
     * check every open window and close/forward those whose covering a time range lesser than
     * windowSize - allowedLateness. The value of allowedLateness will determine how many windows
     * we keep in memory.
     * @param collector the collector to forward closed windows
     */
    private void finalizeOldWindows(Collector<IAccumulator> collector) {
        // windows with a start time older than timeout will be closed.
        long timeout = windowSizeMillis + allowedLateness;
        Set<Long> keys = new LinkedHashSet<>(map.keySet());

        for (Long key : keys) {
            if (key < timeAdvance - timeout) {
                // we have an old window, finalize and close it
                IAccumulator acc = map.get(key);
                acc.finalise();
                collector.collect(acc);
                map.remove(key);
                LOG.trace("finalized window '{}'", acc);
            }
        }//end for
        LOG.trace("cleanup: {}", this);
    }

    /**
     * Close all the windows currently in memory, whatever their timestamp.
     * @param collector the collector to forward measures to
     */
    public void flush(Collector<IAccumulator> collector) {
        for (IAccumulator accumulator : map.values()) {
            accumulator.finalise();
            collector.collect(accumulator);
            LOG.trace("cleanup window '{}'", accumulator);
        }
        map.clear();
    }

    @Override
    public String toString() {
        return String.format("WindowsState{objectId=%d,windows=%d}", objectId, map.size());
    }
}
