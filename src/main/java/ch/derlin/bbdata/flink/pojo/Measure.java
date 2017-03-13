package ch.derlin.bbdata.flink.pojo;

import java.io.Serializable;
import java.util.Date;

/**
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
