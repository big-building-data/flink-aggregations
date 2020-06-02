package ch.derlin.bbdata.flink.mappers;

import ch.derlin.bbdata.flink.AggregationConfiguration;
import ch.derlin.bbdata.flink.pojo.Measure;
import ch.derlin.bbdata.flink.utils.DateUtil;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This mapper converts a measure in json format to a {@link Measure} object and filter measures based on their unit.
 * <p>
 * <p>
 * This means that it will drop any measure: a) whose unit is not of interest
 * (see {@link AggregationConfiguration#isAggregationTarget(Measure)} or b) whose value is not parseable into a float.
 * date: 19/12/16
 *
 * @author "Lucy Linder"
 */
public class StringToFloatMeasureFlatMapper extends RichFlatMapFunction<String, Measure> {

    private static final Logger LOG = LoggerFactory.getLogger(StringToFloatMeasureFlatMapper.class);
    private transient Gson gson;

    @Override
    public void flatMap(String s, Collector<Measure> collector) {
        try {
            Measure m = gson.fromJson(s, Measure.class);
            if (AggregationConfiguration.isAggregationTarget(m)) {
                m.floatValue = Float.parseFloat(m.value);
                if (Float.isNaN(m.floatValue))
                    LOG.warn("NaN encountered. Measure='{}'", s);
                else
                    collector.collect(m);
            }
        } catch (Exception e) {
            LOG.error("{}: {}. Measure='{}'  ", e.getClass().getName(), e.getMessage(), s);
        }
    }

    @Override
    public void open(Configuration parameters) {
        // serializeSpecialFloatingPointValues makes gson handle NaNs, Infinity and special values
        // see https://google.github.io/gson/apidocs/com/google/gson/GsonBuilder.html#serializeSpecialFloatingPointValues--
        DateUtil.setDefaultToUTC();
        gson = new GsonBuilder().serializeSpecialFloatingPointValues().create();
    }
}
