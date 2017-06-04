package ch.derlin.bbdata.flink.accumulators;


import ch.derlin.bbdata.flink.pojo.Measure;

import static java.lang.Float.isNaN;

/**
 * The advanced accumulator will also compute the standard deviation.
 * It is used for objects whose unit is listed in {@link ch.derlin.bbdata.flink.AggregationConfiguration#ADVANCED_AGGR_UNITS}
 * date: 21/12/16
 *
 * @author "Lucy Linder"
 */
public class AdvancedAccumulator extends BasicAccumulator {


    public void fold(Measure m) {
        super.fold(m);
        float v = m.floatValue;

        // stdev
        if (isNaN(k)) {
            k = v;
            kSum = kSumSquared = 0;
        } else {
            // incremental computation of the variance while avoiding catastrophic cancellation
            // see: https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Computing_shifted_data
            kSum += v - k;
            kSumSquared += (v - k) * (v - k);
        }

    }

    @Override
    public void finalise() {
        super.finalise();
        // get the standard deviation
        updateStdDev();
    }


    @Override
    public String toString() {

        return String.format("%s, min=%.2f, std=%f", super.toString(), min, std);

    }

}
