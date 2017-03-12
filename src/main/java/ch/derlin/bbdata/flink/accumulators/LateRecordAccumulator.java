package ch.derlin.bbdata.flink.accumulators;

import ch.derlin.bbdata.flink.pojo.AggregationRecord;
import ch.derlin.bbdata.flink.pojo.Measure;

/**
 * date: 10.03.17
 *
 * @author Lucy Linder <lucy.derlin@gmail.com>
 */
public class LateRecordAccumulator extends AggregationRecord implements IAccumulator {

    public Measure measure;
    public String dateClusteringKey;

    public LateRecordAccumulator(Measure measure, String dateClusteringKey) {
        this.measure = measure;
        this.dateClusteringKey = dateClusteringKey;
        this.count = 1;
    }

    @Override
    public void fold(Measure m) {

    }


    @Override
    public void finalise() {

    }
}
