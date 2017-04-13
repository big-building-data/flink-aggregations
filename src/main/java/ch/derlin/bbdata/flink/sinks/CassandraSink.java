package ch.derlin.bbdata.flink.sinks;

import ch.derlin.bbdata.flink.accumulators.IAccumulator;
import ch.derlin.bbdata.flink.accumulators.LateRecordAccumulator;
import ch.derlin.bbdata.flink.pojo.AggregationRecord;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import org.apache.flink.api.common.time.Time;
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

    protected transient Cluster cluster;
    protected transient Session session;
    protected transient Mapper<AggregationRecord> mapper;
    protected long windowSizeMillis;


    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        try {
            Configuration config = (Configuration) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
            Cluster.Builder builder = Cluster.builder().withRetryPolicy(DefaultRetryPolicy.INSTANCE)
                    //.withLoadBalancingPolicy(new TokenAwarePolicy(new DCAwareRoundRobinPolicy()))
                    // TODO does it improve performances ?
                    ;
            for (String address : config.getString("cassandra.entryPoints", "").split(",")) {
                builder.addContactPoint(address.trim());
            }

            cluster = builder.build();
            session = cluster.connect();

            MappingManager manager = new MappingManager(session);
            mapper = manager.mapper(AggregationRecord.class);

            int windowSizeMinutes = config.getInteger("window.granularity", 15);
            windowSizeMillis = Time.minutes(windowSizeMinutes).toMilliseconds();

        } catch (Exception e) {
            LOGGER.error("setup/open failed", e);
            throw e;
        }
    }

    @Override
    public void invoke(IAccumulator iAccumulator) throws Exception {
        // TODO concurrency ??
        if (iAccumulator instanceof AggregationRecord) {
            AggregationRecord record = iAccumulator.getRecord();

            AggregationRecord oldRecord = mapper.get(record.minutes, record.objectId, record.date, record.timestamp);
            if (oldRecord != null) {
                if (iAccumulator instanceof LateRecordAccumulator) {
                    LOGGER.info("UPDATE => record={} | acc={}", oldRecord, iAccumulator);
                    oldRecord.addOne(record);
                    mapper.save(oldRecord);
                } else {
                    if (oldRecord.count < record.count) {
                        LOGGER.warn("OVERRIDE => overriding: old={} | new={}", oldRecord, record);
                        mapper.save(record);
                    } else {
                        LOGGER.warn("OVERRIDE => skipping: old={} | new={}", oldRecord, record);
                    }
                }
            } else {
                if (iAccumulator instanceof LateRecordAccumulator) {
                    // first time we see this, so we need to un-NaN the k, k_sum etc. fields
                    LOGGER.info("FIRST as late => record={}", record);
                }
                mapper.save(record);
            }

        } else {
            LOGGER.error("Got something else than an aggregation record: {} -- {} ", iAccumulator.getClass(), iAccumulator);
        }
    }


    @Override
    public void close() {
        cluster.closeAsync();
    }

}
