package ch.derlin.bbdata.flink.window;

import ch.derlin.bbdata.flink.accumulators.IAccumulator;
import ch.derlin.bbdata.flink.pojo.Measure;
import ch.derlin.bbdata.flink.utils.DateUtil;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * The Window Mapper instance will handle all measure from a given source (i.e. with the same {@link Measure#objectId}).
 * It is a stateful mapper, with a state of type {@link WindowState}.
 * <p>
 * <p>
 * date: 06.03.17
 *
 * @author Lucy Linder <lucy.derlin@gmail.com>
 */
public class WindowMapper extends KeyedProcessFunction<Tuple, Measure, IAccumulator> {

    private Logger LOG = LoggerFactory.getLogger(WindowMapper.class);

    /**
     * The current state
     */
    protected transient ValueState<WindowState> state;
    // The following two variables are used to handle cases when an object stops sending measures.
    // In this case, we need to flush the opened windows, to avoid them to stay in memory indefinitely

    // after how many minutes do we consider the object has stopped sending records and the windows need to
    // be flushed (in ms)
    private transient long timeout;
    // the window granularity, in ms
    private transient long granularity;
    // the lateness allowed, that is how long do we keep a window in memory after its window is passed, in ms
    private transient long allowedLateness;

    // the last time the mapper received a measure (system time)
    private transient long lastProcessingTime;
    // whether or not a timer is running.
    private transient boolean timerStarted = false;

    @Override
    public void processElement(Measure measure, Context context, Collector<IAccumulator> collector) throws Exception {
        WindowState currentState = getOrCreateState();
        currentState.process(collector, measure);
        state.update(currentState);

        lastProcessingTime = context.timerService().currentProcessingTime();
        if (!timerStarted) {
            // first record since the program started, start the timer
            context.timerService().registerProcessingTimeTimer(context.timerService().currentProcessingTime() + timeout);
            timerStarted = true;
        }
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        // ensure the JVM and JodaTime are configured for UTC dates
        DateUtil.setDefaultToUTC();
        // define configuration
        // all those options are in minutes
        ConfigOption<Integer> configGranularity = ConfigOptions.key("window.granularity").intType().defaultValue(15);
        ConfigOption<Integer> configLateness = ConfigOptions.key("window.allowed_lateness").intType().defaultValue(5);
        ConfigOption<Integer> configTimeout = ConfigOptions.key("window.timeout").intType().defaultValue(10);

        // extract configuration properties from the context
        Configuration config = (Configuration) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        granularity = Time.minutes(config.get(configGranularity)).toMilliseconds();
        allowedLateness = Time.minutes(config.get(configLateness)).toMilliseconds();
        timeout = Time.minutes(config.get(configTimeout)).toMilliseconds();

        // fetch the state from Flink backend
        ValueStateDescriptor<WindowState> descriptor =
                new ValueStateDescriptor<>(
                        "AccWindowState_" + granularity, // the state name
                        TypeInformation.of(WindowState.class) // type information
                );
        state = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext context, Collector<IAccumulator> collector) throws Exception {
        // check if we received any record in the last "timeout" minutes
        if (timestamp - lastProcessingTime >= timeout) {
            // nothing happened for a while, flush the windows in memory
            WindowState currentState = state.value();
            if (currentState != null) {
                LOG.trace("timeout: triggering cleanup of {}", currentState);
                currentState.flush(collector);
                state.update(currentState);
            }
        }
        // start a new alarm
        context.timerService().registerProcessingTimeTimer(context.timerService().currentProcessingTime() + timeout);
    }

    private WindowState getOrCreateState() throws IOException {
        WindowState windowState = state.value();
        return windowState == null ? new WindowState(granularity, allowedLateness) : windowState;
    }
}
