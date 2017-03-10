package ch.derlin.bbdata.flink.window;

import ch.derlin.bbdata.commons.dateutils.TimeZoneUtils;
import ch.derlin.bbdata.flink.accumulators.IAccumulator;
import ch.derlin.bbdata.flink.pojo.Measure;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.RichProcessFunction;
import org.apache.flink.util.Collector;

/**
 * date: 06.03.17
 *
 * @author Lucy Linder <lucy.derlin@gmail.com>
 */
public class WindowMapper extends RichProcessFunction<Measure, IAccumulator> {

    protected transient ValueState<WindowState> state;
    private transient long timeout, lastProcessingTime;
    private transient boolean timerStarted = false;

    @Override
    public void processElement(Measure measure, Context context, Collector<IAccumulator> collector) throws Exception {
        WindowState currentState = state.value();
        currentState.process(collector, measure);
        state.update(currentState);

        lastProcessingTime = context.timerService().currentProcessingTime();
        if (!timerStarted) {
            context.timerService().registerProcessingTimeTimer(context.timerService().currentProcessingTime() + timeout);
            timerStarted = true;
        }
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        TimeZoneUtils.setDefaultToUTC();
        Configuration config = (Configuration) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        int windownMinutes = config.getInteger("window.granularity", 15);
        int allowedLatenessMinutes = config.getInteger("window.allowed_lateness", 5);
        int timeoutMinutes = config.getInteger("window.timeout", 3);
        timeout = Time.minutes(timeoutMinutes).toMilliseconds();

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
        if (timestamp - lastProcessingTime >= timeout) {
            WindowState currentState = state.value();
            System.out.println("CLEANUP TRIGGERED for " + currentState);
            currentState.flush(collector);
            state.update(currentState);
        }
        context.timerService().registerProcessingTimeTimer(context.timerService().currentProcessingTime() + timeout);
    }

}
