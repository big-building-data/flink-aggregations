package ch.derlin.bbdata.flink.mappers;

import ch.derlin.bbdata.commons.GsonProvider;
import ch.derlin.bbdata.commons.dateutils.TimeZoneUtils;
import ch.derlin.bbdata.flink.AggregationConfiguration;
import ch.derlin.bbdata.flink.Main;
import ch.derlin.bbdata.flink.pojo.Measure;
import com.google.gson.Gson;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This mapper converts a measure in json format to a {@link Measure} object and filter measures based on their unit.
 *
 *
 * This means that it will drop any measure: a) whose unit is not of interest
 * (see {@link AggregationConfiguration#isAggregationTarget(Measure)} or b) whose value is not parseable into a float.
 * date: 19/12/16
 *
 * @author "Lucy Linder"
 */
public class StringToFloatMeasureFlatMapper extends RichFlatMapFunction<String, Measure> {

    private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);
    private transient Gson gson;

    @Override
    public void flatMap(String s, Collector<Measure> collector) throws Exception {
        try {
            Measure m = gson.fromJson(s, Measure.class);
            if (AggregationConfiguration.isAggregationTarget(m)) {
                m.floatValue = Float.parseFloat(m.value);
                collector.collect(m);
            }
        } catch (Exception e) {
            LOGGER.error("deserializing '{}'  ", s, e);
        }
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        // serializeSpecialFloatingPointValues makes gson handle NaNs, Infinity and special values
        // see https://google.github.io/gson/apidocs/com/google/gson/GsonBuilder.html#serializeSpecialFloatingPointValues--
        TimeZoneUtils.setDefaultToUTC();
        gson = GsonProvider.getBuilder().serializeSpecialFloatingPointValues().create();
    }
}
