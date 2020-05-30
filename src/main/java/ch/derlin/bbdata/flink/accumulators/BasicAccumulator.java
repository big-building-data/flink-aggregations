package ch.derlin.bbdata.flink.accumulators;


import ch.derlin.bbdata.flink.pojo.AggregationRecord;
import ch.derlin.bbdata.flink.pojo.Measure;
import ch.derlin.bbdata.flink.utils.DateUtil;

import static java.lang.Float.isNaN;

/**
 * The basic accumulator will compute:
 * <ul>
 * <li>sum</li>
 * <li>count</li>
 * <li>mean</li>
 * <li>min</li>
 * <li>max</li>
 * </ul>
 * <p>
 * It is used for objects with a unit matching ({@link ch.derlin.bbdata.flink.AggregationConfiguration#BASIC_AGGR_UNITS}).
 * <p>
 * date: 10.03.17
 *
 * @author Lucy Linder <lucy.derlin@gmail.com>
 */
public class BasicAccumulator extends AggregationRecord implements IAccumulator {
    /**
     * The start time of the window, in milliseconds since epoch.
     */
    public long windowTime;
    protected boolean withStdev;

    public BasicAccumulator() {
        this.withStdev = false;
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


    @Override
    public String toString() {
        return String.format("[%4d, %s] count=%d, max=%.2f, mean=%.2f", //
                objectId, DateUtil.dateToString(windowTime), count, max, mean);
    }
}
