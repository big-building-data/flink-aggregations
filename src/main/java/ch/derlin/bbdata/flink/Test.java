package ch.derlin.bbdata.flink;

import ch.derlin.bbdata.commons.dateutils.DateTimeFormatUTC;
import ch.derlin.bbdata.flink.pojo.AggregationRecord;
import ch.derlin.bbdata.flink.accumulators.BasicAccumulator;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import org.apache.flink.api.java.utils.ParameterTool;

/**
 * date: 10.03.17
 *
 * @author Lucy Linder <lucy.derlin@gmail.com>
 */
public class Test {

    static Cluster cluster;
    static Session session;
    static DateTimeFormatUTC dt = new DateTimeFormatUTC();
    
    public static void main(String[] args) throws Exception {
        testMapper();
    }//end main


    public static void testMapper() throws Exception {
        open();
        MappingManager manager = new MappingManager(session);
        Mapper<AggregationRecord> mapper = manager.mapper(AggregationRecord.class);
        BasicAccumulator acc = new BasicAccumulator();
        acc.objectId = 4933;
        acc.date = "2017-02";
        acc.timestamp = dt.parse("2017-02-03T00:00");
        acc.count = 2;
        acc.sum = 4;
        acc.k = 3;
        acc.kSum = 2;
        acc.kSumSquared = 4;

        AggregationRecord old = mapper.get(acc.objectId, acc.date, acc.timestamp);
        if (old != null) {
            System.out.printf("update: i=%d, old=%d%n", acc.count, old.count);
            acc.count += old.count;
            mapper.save(acc);
        } else {
            mapper.save(acc);
        }

        System.out.println("done");
    }

    public static void open() throws Exception {

        // get global config
        ParameterTool config = ParameterTool.fromPropertiesFile("/Users/Lin/git/BBDATA2/flink-aggregations/local.properties");

        Cluster.Builder builder = Cluster.builder().withRetryPolicy(DefaultRetryPolicy.INSTANCE)
                //.withLoadBalancingPolicy(new TokenAwarePolicy(new DCAwareRoundRobinPolicy())) // TODO does it
                // improve performances ?
                ;
        for (String address : config.getRequired("cassandra.entryPoints").split(",")) {
            builder.addContactPoint(address.trim());
        }

        cluster = builder.build();
        session = cluster.connect(config.getRequired("cassandra.schema"));

    }
}
