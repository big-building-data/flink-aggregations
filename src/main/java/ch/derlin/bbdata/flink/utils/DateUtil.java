package ch.derlin.bbdata.flink.utils;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

/**
 * Some basic utilities to log dates.
 * date: 01/01/17
 *
 * @author "Lucy Linder"
 */
public class DateUtil {

    public static final String DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss'Z'";

    private static SimpleDateFormat formatter = null; // do not initialize here: let the setDefaultToUTC be called first


    public static String dateToString(Long t) {
        return t == null ? "null" : new DateTime(t).toString("YYYY-MM-dd'T'HH:mm:ss");
    }

    public static String dateToString(Date d) {
        if (formatter == null) formatter = new SimpleDateFormat(DATE_FORMAT);
        return d == null ? "null" : formatter.format(d);
    }

    public static void setDefaultToUTC() {
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        DateTimeZone.setDefault(DateTimeZone.UTC);
    }

    public static int ms2Minutes(long millis) {
        return (int) (millis / 60000);
    }

}
