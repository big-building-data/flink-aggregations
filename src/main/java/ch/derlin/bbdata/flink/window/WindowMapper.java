package ch.derlin.bbdata.flink.window;

import ch.derlin.bbdata.flink.accumulators.IAccumulator;
import ch.derlin.bbdata.flink.pojo.Measure;
import ch.derlin.bbdata.flink.utils.DateUtil;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The Window Mapper instance will handle all measure from a given source (i.e. with the same {@link Measure#objectId}).
 * It is a stateful mapper, with a state of type {@link WindowState}.
 *
 *
 * date: 06.03.17
 *
 * @author Lucy Linder <lucy.derlin@gmail.com>
 */
public class WindowMapper extends ProcessFunction<Measure, IAccumulator> {

    private Logger LOG = LoggerFactory.getLogger( WindowMapper.class );

    /**
     * The current state
     */
    protected transient ValueState<WindowState> state;
    // The following two variables are used to handle cases when an object stops sending measures.
    // In this case, we need to flush the opened windows, to avoid them to stay in memory indefinitely

    // after how many minutes do we consider the object has stopped sending records and the windows need to
    // be flushed
    private transient long timeout;
    // the last time the mapper received a measure (system time)
    private transient  long lastProcessingTime;
    // whether or not a timer is running.
    private transient boolean timerStarted = false;

    @Override
    public void processElement(Measure measure, Context context, Collector<IAccumulator> collector) throws Exception {
        WindowState currentState = state.value();
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
        // extract configuration properties from the context
        Configuration config = (Configuration) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        int windownMinutes = config.getInteger("window.granularity", 15);
        int allowedLatenessMinutes = config.getInteger("window.allowed_lateness", 5);
        int timeoutMinutes = config.getInteger("window.timeout", 3);
        timeout = Time.minutes(timeoutMinutes).toMilliseconds();

        // fetch the state from Flink backend
        ValueStateDescriptor<WindowState> descriptor =
                new ValueStateDescriptor<>(
                        "AccWindowState_" + windownMinutes, // the state name
                        TypeInformation.of(WindowState.class), // type information
                        new WindowState(                     // default value of the state, if nothing was set
                                Time.minutes(windownMinutes).toMilliseconds(),
                                Time.minutes(allowedLatenessMinutes).toMilliseconds()));


        state = getRuntimeContext().getState(descriptor);

    }

    @Override
    public void onTimer(long timestamp, OnTimerContext context, Collector<IAccumulator> collector) throws Exception {
        // check if we received any record in the last "timeout" minutes
        if (timestamp - lastProcessingTime >= timeout) {
            // nothing happened for a while, flush the windows in memory
            WindowState currentState = state.value();
            LOG.trace("timeout: triggering cleanup of {}", currentState);
            currentState.flush(collector);
            state.update(currentState);
        }
        // start a new alarm
        context.timerService().registerProcessingTimeTimer(context.timerService().currentProcessingTime() + timeout);
    }

}
