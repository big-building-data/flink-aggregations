package ch.derlin.bbdata.flink.accumulators;

import ch.derlin.bbdata.flink.pojo.AggregationRecord;
import ch.derlin.bbdata.flink.pojo.Measure;

/**
 * date: 10.03.17
 *
 * @author Lucy Linder <lucy.derlin@gmail.com>
 */
public class LateRecordAccumulator implements IAccumulator {

    public String dateClusteringKey;
    public AggregationRecord record;

    public LateRecordAccumulator(AggregationRecord r, String dateClusteringKey) {
        this.dateClusteringKey = dateClusteringKey;
        this.record = r;
    }

    @Override
    public void fold(Measure m) {

    }


    @Override
    public void finalise() {

    }

    @Override
    public AggregationRecord getRecord() {
        return record;
    }
}
