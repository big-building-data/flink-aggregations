package ch.derlin.bbdata.flink;

import ch.derlin.bbdata.flink.accumulators.Accumulator;
import ch.derlin.bbdata.flink.accumulators.IAccumulator;
import ch.derlin.bbdata.flink.accumulators.LateRecordAccumulator;
import ch.derlin.bbdata.flink.pojo.Measure;
import org.joda.time.DateTime;

import java.util.Arrays;
import java.util.List;

import static ch.derlin.bbdata.flink.utils.DateUtil.ms2Minutes;

/**
 * This class
 * date: 10.03.17
 *
 * @author Lucy Linder <lucy.derlin@gmail.com>
 */
public class AggregationConfiguration {

    // filtering TODO: why ???
    public static final List<String> BASIC_AGGR_UNITS = Arrays.asList("lx,m/s".split(","));
    public static final List<String> ADVANCED_AGGR_UNITS = Arrays.asList("%,A,ppm,V,W,°C".split(","));

    public static boolean isAggregationTarget(Measure m) {
        return BASIC_AGGR_UNITS.contains(m.unitSymbol) || ADVANCED_AGGR_UNITS.contains(m.unitSymbol);
    }

    public static IAccumulator getLateAccumulatorFor(Measure m, long windowStart, long windowSizeMillis) {
        assert isAggregationTarget(m);
        // create an accumulator and fold+finalize once to account for the single measure m
        IAccumulator acc = getAccumulatorFor(m, windowStart, windowSizeMillis);
        acc.fold(m);
        acc.finalise();
        // then, wrap the accumulator in a lateRecord
        return new LateRecordAccumulator(acc.getRecord());
    }

    public static IAccumulator getAccumulatorFor(Measure m, long windowStart, long windowSizeMillis) {
        assert isAggregationTarget(m);
        String clusteringKey = getDateClusteringKeyFor(windowStart, windowSizeMillis);
        boolean withStdev = (ADVANCED_AGGR_UNITS.contains(m.unitSymbol));
        return new Accumulator(windowStart, windowSizeMillis, clusteringKey, m.objectId, withStdev);
    }


    public static long getWindowStartFor(Measure m, long windowSizeMillis) {
        return getWindowStart(m.timestamp.getTime(), windowSizeMillis);
    }

    public static long getWindowStart(long timestamp, long windowSizeMillis) {
        DateTime dt = new DateTime(timestamp);
        int minutes = ms2Minutes(windowSizeMillis);
        return dt.withMinuteOfHour((dt.getMinuteOfHour() / minutes) * minutes)
                .minuteOfDay().roundFloorCopy().getMillis();
    }

    public static String getDateClusteringKeyFor(long windowStart, long windowSizeMillis) {
        DateTime d = new DateTime(windowStart);
        if (ms2Minutes(windowSizeMillis) <= 60) {
            // one hour or less: partition by month
            return String.format("%4d-%02d", d.getYear(), d.getMonthOfYear());
        } else {
            // more than one hour: partition by slices of 6 months
            return String.format("%4d-%02d", d.getYear(), d.getMonthOfYear() <= 6 ? 1 : 12);
        }
    }
}
