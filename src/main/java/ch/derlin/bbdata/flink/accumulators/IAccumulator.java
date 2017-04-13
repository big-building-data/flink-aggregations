package ch.derlin.bbdata.flink.accumulators;

import ch.derlin.bbdata.flink.pojo.AggregationRecord;
import ch.derlin.bbdata.flink.pojo.Measure;

/**
 * date: 10.03.17
 *
 * @author Lucy Linder <lucy.derlin@gmail.com>
 */
public interface IAccumulator {


    void fold(Measure m);

    void finalise();

    AggregationRecord getRecord();
}

