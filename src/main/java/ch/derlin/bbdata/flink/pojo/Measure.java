package ch.derlin.bbdata.flink.pojo;

import java.io.Serializable;
import java.util.Date;
import java.util.Random;

/**
 * date: 19/12/16
 *
 * @author "Lucy Linder"
 */
public class Measure implements Serializable{

    private static final String[] AVAILABLE_UNITS = "%,A,ppm,V,W,°C,lx,measure/s,Wh".split( "," );


    public static final Random r = new Random( new Date().getTime() );

    public Integer objectId;
    public String unitSymbol;
    public Date timestamp;
    public String value;
    public String type;
    public float floatValue = Float.NaN;
    public String accType;


    public Measure(boolean auto ){
        objectId = r.nextInt( 5 );
        timestamp = new Date();
        unitSymbol = AVAILABLE_UNITS[ r.nextInt( AVAILABLE_UNITS.length ) ];
        value = String.valueOf(r.nextFloat() * 10);
    }

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
