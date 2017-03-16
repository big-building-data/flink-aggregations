package ch.derlin.bbdata.flink;

import ch.derlin.bbdata.flink.accumulators.AdvancedAccumulator;
import ch.derlin.bbdata.flink.accumulators.BasicAccumulator;
import ch.derlin.bbdata.flink.accumulators.IAccumulator;
import ch.derlin.bbdata.flink.accumulators.LateRecordAccumulator;
import ch.derlin.bbdata.flink.pojo.Measure;
import org.joda.time.DateTime;

import java.util.Arrays;
import java.util.Date;
import java.util.List;

/**
 * date: 10.03.17
 *
 * @author Lucy Linder <lucy.derlin@gmail.com>
 */
public class AggregationConfiguration {

    // filtering
    public static final List<String> BASIC_AGGR_UNITS = Arrays.asList("lx,m/s".split(","));
    public static final List<String> ADVANCED_AGGR_UNITS = Arrays.asList("%,A,ppm,V,W,°C".split(","));

    public static boolean isAggregationTarget(Measure m) {
        return BASIC_AGGR_UNITS.contains(m.unitSymbol) || ADVANCED_AGGR_UNITS.contains(m.unitSymbol);
    }

    public static IAccumulator getLateAccumulatorFor(Measure m, long windowStart, long windowSizeMillis) {
        assert isAggregationTarget(m);
        LateRecordAccumulator acc = new LateRecordAccumulator(m, getDateClusteringKeyFor(windowStart, windowSizeMillis));
        acc.objectId = m.objectId;
        acc.minutes = toMinutes(windowSizeMillis);
        acc.timestamp = new Date(windowStart);
        acc.date = getDateClusteringKeyFor(windowStart, windowSizeMillis);
        return acc;
    }

    public static IAccumulator getAccumulatorFor(Measure m, long windowStart, long windowSizeMillis) {
        assert isAggregationTarget(m);
        BasicAccumulator acc;

        if (BASIC_AGGR_UNITS.contains(m.unitSymbol)) acc = new BasicAccumulator();
        else acc = new AdvancedAccumulator();

        acc.windowTime = windowStart;
        acc.objectId = m.objectId;
        acc.timestamp = new Date(windowStart);
        acc.minutes = toMinutes(windowSizeMillis);
        acc.date = getDateClusteringKeyFor(windowStart, windowSizeMillis);
        return acc;
    }


    public static long getWindowStartFor(Measure m, long windowSizeMillis) {
        return getWindowStart(m.timestamp.getTime(), windowSizeMillis);
    }

    public static long getWindowStart(long timestamp, long windowSizeMillis) {
        DateTime dt = new DateTime(timestamp);
        int minutes = toMinutes(windowSizeMillis);
        return dt.withMinuteOfHour((dt.getMinuteOfHour() / minutes) * minutes)
                .minuteOfDay().roundFloorCopy().getMillis();
    }

    public static String getDateClusteringKeyFor(long windowStart, long windowSizeMillis) {
        DateTime d = new DateTime(windowStart);
        if (toMinutes(windowSizeMillis) <= 60) {
            // one hour or less: partition by month
            return String.format("%4d-%02d", d.getYear(), d.getMonthOfYear());
        } else {
            // more than one hour: partition by slices of 6 months
            return String.format("%4d-%02d", d.getYear(), d.getMonthOfYear() > 6 ? 1 : 12);
        }
    }

    public static int toMinutes(long millis) {
        return (int) (millis / 60000);
    }

}
