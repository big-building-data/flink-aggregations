package ch.derlin.bbdata.flink.pojo;

import java.io.Serializable;
import java.util.Date;

/**
 * A Measure. Most of the fields will be instantiated by GSON upon json deserialization
 * (see {@link ch.derlin.bbdata.flink.mappers.StringToFloatMeasureFlatMapper}).
 * The only "special" field is {@link #floatValue}, which is not part of the measure per se, but will hold
 * the {@link #value} after parsing (since value is a string, it cannot be done automatically by gson).
 * date: 19/12/16
 *
 * @author "Lucy Linder"
 */
public class Measure implements Serializable{

    public Integer objectId;
    public String unitSymbol;
    public Date timestamp;
    public String value;
    public String type;
    public float floatValue = Float.NaN;


    public Measure(){}


    @Override
    public String toString(){
        return "Measure{" +
                "objectId=" + objectId +
                ", timestamp=" + timestamp +
                ", value=" + value +
                ", floatValue=" + floatValue +
                '}';
    }

}
