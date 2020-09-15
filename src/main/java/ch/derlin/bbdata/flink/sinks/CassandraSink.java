package ch.derlin.bbdata.flink.sinks;

import ch.derlin.bbdata.flink.Configs;
import ch.derlin.bbdata.flink.accumulators.IAccumulator;
import ch.derlin.bbdata.flink.accumulators.LateRecordAccumulator;
import ch.derlin.bbdata.flink.pojo.AggregationRecord;
import ch.derlin.bbdata.flink.utils.DateUtil;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.datastax.driver.mapping.Mapper.Option.consistencyLevel;

/**
 * date: 10.03.17
 *
 * @author Lucy Linder <lucy.derlin@gmail.com>
 */
public class CassandraSink extends RichSinkFunction<IAccumulator> {

    protected static final Logger LOG = LoggerFactory.getLogger(CassandraSink.class);

    // transient properties are never serialized. Each node will recreate them
    // in the open/close methods of the RichSinkFunction interface
    protected transient Cluster cluster;
    protected transient Session session;
    protected transient Mapper<AggregationRecord> mapper;

    // metrics
    private transient Counter normalCounter,
            lateCounter, lateUpdateCounter, lateFirstCounter,
            overrideCounter, skipCounter;

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
                LOG.info("adding cassandra contact point: " + address.trim());
                builder.addContactPoint(address.trim());
            }

            // set consistency
            QueryOptions queryOptions = new QueryOptions();
            queryOptions.setConsistencyLevel(Configs.readCassandraConsistency(config));

            // the keyspace and tables are declared in the AggregationRecord class
            // so here, just create a connection
            cluster = builder
                    .withQueryOptions(queryOptions)
                    .build();
            session = cluster.connect();

            // the mapper will do the mapping between cassandra records and AggregationRecord objects
            MappingManager manager = new MappingManager(session);
            mapper = manager.mapper(AggregationRecord.class);


        } catch (Exception e) {
            LOG.error("setup/open failed", e);
            throw e;
        }

        // setup metrics
        // (no prefix, as 0.Sink__CassandraSink will be added automatically

        // number of "normal cases"
        normalCounter = getRuntimeContext().getMetricGroup().counter("rec_normal");
        // number of late record, whether a previous record exist or not
        lateCounter = getRuntimeContext().getMetricGroup().counter("rec_late");
        // number of late records, no previous window existed
        lateFirstCounter = getRuntimeContext().getMetricGroup().counter("rec_late_first");
        // number of late records, where a previous window existed (hence is updated)
        lateUpdateCounter = getRuntimeContext().getMetricGroup().counter("rec_late_update");
        // number of records where a previous window existed and this new records overrode
        overrideCounter = getRuntimeContext().getMetricGroup().counter("rec_override");
        // number of records where a previous window existed and this record was skipped
        skipCounter = getRuntimeContext().getMetricGroup().counter("rec_skip");
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
                // update counters
                LOG.debug("UPDATE => record={} | acc={}", oldRecord, iAccumulator);
                lateCounter.inc();
                lateUpdateCounter.inc();
                // update record
                oldRecord.addOne(record);
                mapper.save(oldRecord);
            } else {
                // this might be a duplicate, or a partial record due to a restart of the program
                // without a saved state => replace the record already in cassandra only if this
                // record has more measures
                if (oldRecord.count < record.count) {
                    LOG.debug("OVERRIDE => overriding: old={} | new={}", oldRecord, record);
                    overrideCounter.inc();
                    mapper.save(record);
                } else {
                    LOG.debug("OVERRIDE => skipping: old={} | new={}", oldRecord, record);
                    skipCounter.inc();
                }
            }
        } else {
            // simply save the new record
            if (iAccumulator instanceof LateRecordAccumulator) {
                // first time we see this
                LOG.debug("FIRST as late => record={}", record);
                lateCounter.inc();
                lateFirstCounter.inc();
            } else {
                normalCounter.inc();
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
