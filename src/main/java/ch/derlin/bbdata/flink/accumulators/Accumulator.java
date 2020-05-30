package ch.derlin.bbdata.flink.accumulators;


import ch.derlin.bbdata.flink.pojo.AggregationRecord;
import ch.derlin.bbdata.flink.pojo.Measure;
import ch.derlin.bbdata.flink.utils.DateUtil;

import java.util.Date;

import static java.lang.Float.isNaN;

/**
 * The accumulator will always compute:
 * <ul>
 * <li>sum</li>
 * <li>count</li>
 * <li>mean</li>
 * <li>min</li>
 * <li>max</li>
 * <li>lastMeasure</li>
 * <li>lastTimestamp</li>
 * </ul>
 * And compute the standard deviation ONLY if `withStdev` is true (default):
 * <p>
 * It should  be created only by ({@link ch.derlin.bbdata.flink.AggregationConfiguration#getAccumulatorFor(Measure, long, long)}).
 * <p>
 * date: 10.03.17
 *
 * @author Lucy Linder <lucy.derlin@gmail.com>
 */
public class Accumulator extends AggregationRecord implements IAccumulator {
    /**
     * The start time of the window, in milliseconds since epoch.
     */
    protected boolean withStdev;

    public Accumulator(long windowStart, long windowSizeMillis, String clusteringKey, int objectId, boolean withStdev) {
        this.objectId = objectId;
        this.timestamp = new Date(windowStart);
        this.minutes = DateUtil.ms2Minutes(windowSizeMillis);
        this.date = clusteringKey;
        this.withStdev = withStdev;
    }

    public Accumulator(long windowStart, long windowSizeMillis, String clusteringKey, int objectId) {
        this(windowStart, windowSizeMillis, clusteringKey, objectId, true);
    }

    @Override
    public void fold(Measure m) {
        addValue(m.floatValue, m.timestamp.getTime(), withStdev);
    }

    @Override
    public void finalise() {
        computeFinalValues();
    }

    @Override
    public AggregationRecord getRecord() {
        return this;
    }

    public boolean isWithStdev() {
        return withStdev;
    }

    @Override
    public String toString() {
        return String.format("[%4d, %s] count=%d, max=%.2f, mean=%.2f", //
                objectId, DateUtil.dateToString(timestamp), count, max, mean);
    }
}
