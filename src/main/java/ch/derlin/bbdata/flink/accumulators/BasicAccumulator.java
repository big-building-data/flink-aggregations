package ch.derlin.bbdata.flink.accumulators;


import ch.derlin.bbdata.flink.pojo.AggregationRecord;
import ch.derlin.bbdata.flink.pojo.Measure;
import ch.derlin.bbdata.flink.utils.DateUtil;

import static java.lang.Float.isNaN;

/**
 * date: 10.03.17
 *
 * @author Lucy Linder <lucy.derlin@gmail.com>
 */
public class BasicAccumulator extends AggregationRecord implements IAccumulator {

    public long windowTime;

    @Override
    public void fold(Measure m) {
        // min
        if (isNaN(min) || min > m.floatValue) min = m.floatValue;
        if (isNaN(max) || max < m.floatValue) max = m.floatValue;
        count += 1;
        sum += m.floatValue;
        if (lastMeasureTimestamp < m.timestamp.getTime()) {
            lastMeasure = m.floatValue;
            lastMeasureTimestamp = m.timestamp.getTime();
        }
    }

    @Override
    public void finalise() {
       updateMean();
    }


    @Override
    public String toString() {
        return String.format("[%4d, %s] count=%d, max=%.2f, mean=%.2f", //
                objectId, DateUtil.dateToString(windowTime), count, max, getMean());
    }
}
