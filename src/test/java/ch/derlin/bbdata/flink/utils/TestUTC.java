package ch.derlin.bbdata.flink.utils;

import org.junit.jupiter.api.BeforeAll;

/**
 * date: 29.05.20
 *
 * @author Lucy Linder <lucy.derlin@gmail.com>
 */
public interface TestUTC {
    @BeforeAll
    static void setTimezone() {
        DateUtil.setDefaultToUTC();
    }
}
