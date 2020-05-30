package ch.derlin.bbdata.flink.sinks;

import ch.derlin.bbdata.flink.Configs;
import ch.derlin.bbdata.flink.accumulators.IAccumulator;
import ch.derlin.bbdata.flink.accumulators.LateRecordAccumulator;
import ch.derlin.bbdata.flink.pojo.AggregationRecord;
import ch.derlin.bbdata.flink.utils.DateUtil;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * date: 10.03.17
 *
 * @author Lucy Linder <lucy.derlin@gmail.com>
 */
public class CassandraSink extends RichSinkFunction<IAccumulator> {

    protected static final Logger LOGGER = LoggerFactory.getLogger(CassandraSink.class);

    // transient properties are never serialized. Each node will recreate them
    // in the open/close methods of the RichSinkFunction interface
    protected transient Cluster cluster;
    protected transient Session session;
    protected transient Mapper<AggregationRecord> mapper;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        DateUtil.setDefaultToUTC();

        try {
            Configuration config = (Configuration) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
            Cluster.Builder builder = Cluster.builder().withRetryPolicy(DefaultRetryPolicy.INSTANCE)
                    //.withLoadBalancingPolicy(new TokenAwarePolicy(new DCAwareRoundRobinPolicy()))
                    // TODO does it improve performances ?
                    ;
            for (String address : Configs.readCassandraEntrypoints(config)) {
                builder.addContactPoint(address.trim());
            }

            // the keyspace and tables are declared in the AggregationRecord class
            // so here, just create a connection
            cluster = builder.build();
            session = cluster.connect();

            // the mapper will do the mapping between cassandra records and AggregationRecord objects
            MappingManager manager = new MappingManager(session);
            mapper = manager.mapper(AggregationRecord.class);

        } catch (Exception e) {
            LOGGER.error("setup/open failed", e);
            throw e;
        }
    }

    @Override
    public void invoke(IAccumulator iAccumulator, Context c) {
        // TODO concurrency ??
        // get the record
        AggregationRecord record = iAccumulator.getRecord();
        // check if a record already exists in cassandra
        AggregationRecord oldRecord = mapper.get(record.minutes, record.objectId, record.date, record.timestamp);

        if (oldRecord != null) {
            // there is already a record for this window in cassandra
            if (iAccumulator instanceof LateRecordAccumulator) {
                // this is a late record, i.e. a single entry => update the stats in cassandra
                LOGGER.debug("UPDATE => record={} | acc={}", oldRecord, iAccumulator);
                oldRecord.addOne(record);
                mapper.save(oldRecord);
            } else {
                // this might be a duplicate, or a partial record due to a restart of the program
                // without a saved state => replace the record already in cassandra only if this
                // record has more measures
                if (oldRecord.count < record.count) {
                    LOGGER.debug("OVERRIDE => overriding: old={} | new={}", oldRecord, record);
                    mapper.save(record);
                } else {
                    LOGGER.debug("OVERRIDE => skipping: old={} | new={}", oldRecord, record);
                }
            }
        } else {
            // simply save the new record
            if (iAccumulator instanceof LateRecordAccumulator) {
                // first time we see this, so we need to un-NaN the k, k_sum etc. fields
                LOGGER.debug("FIRST as late => record={}", record);
            }
            mapper.save(record);
        }
    }


    @Override
    public void close() {
        if (cluster != null)
            cluster.closeAsync();
    }

}
