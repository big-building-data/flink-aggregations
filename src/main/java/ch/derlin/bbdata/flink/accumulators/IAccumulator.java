package ch.derlin.bbdata.flink.accumulators;

import ch.derlin.bbdata.flink.pojo.AggregationRecord;
import ch.derlin.bbdata.flink.pojo.Measure;

/**
 * Accumulators are used by the {@link ch.derlin.bbdata.flink.window.WindowMapper}.
 * The latter will call fold() for each new measure, while the finalise() method will be called when the window
 * is closed (right before forwarding it to a sink).
 * 
 * date: 10.03.17
 *
 * @author Lucy Linder <lucy.derlin@gmail.com>
 */
public interface IAccumulator {


    void fold(Measure m);

    void finalise();

    AggregationRecord getRecord();
}

