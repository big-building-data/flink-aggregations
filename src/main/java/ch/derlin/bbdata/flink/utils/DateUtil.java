package ch.derlin.bbdata.flink.utils;

import org.joda.time.DateTime;

import java.util.Date;

/**
 * date: 01/01/17
 *
 * @author "Lucy Linder"
 */
public class DateUtil {

    public static DateTime floorMinutes(long millis) {
        return new DateTime(millis).minuteOfHour().roundFloorCopy();
    }

    public static DateTime floorMinutes(Date date) {
        return new DateTime(date).minuteOfHour().roundFloorCopy();
    }

    public static DateTime floorQuarter(Date date) {
        DateTime dt = new DateTime(date);
        return dt.withMinuteOfHour((dt.getMinuteOfHour() / 15) * 15).minuteOfDay().roundFloorCopy();
    }


    public static String dateToString(Long t) {
        return t == null ? "null" : new DateTime(t).toString("HH:mm:ss");
    }


    public static String dateToStringM(Long t) {
        return t == null ? "null" : new DateTime(t).toString("HH:mm:ss.SSS");
    }


}
