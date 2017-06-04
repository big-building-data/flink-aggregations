package ch.derlin.bbdata.flink.accumulators;

import ch.derlin.bbdata.flink.pojo.AggregationRecord;
import ch.derlin.bbdata.flink.pojo.Measure;

/**
 * This accumulator is especially made for storing one single measure belonging to an already closed window.
 * It is not an accumulator per se and should be forwarded directly to the sink.
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
        // do nothing
    }


    @Override
    public void finalise() {
        // do nothing
    }

    @Override
    public AggregationRecord getRecord() {
        return record;
    }
}
