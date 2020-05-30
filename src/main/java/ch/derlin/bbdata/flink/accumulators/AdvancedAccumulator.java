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

    public AdvancedAccumulator() {
        this.withStdev = true;
    }

    @Override
    public String toString() {

        return String.format("%s, min=%.2f, std=%f", super.toString(), min, std);

    }

}
