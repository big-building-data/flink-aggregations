package ch.derlin.bbdata.flink.pojo;

import ch.derlin.bbdata.flink.utils.DateUtil;
import com.datastax.driver.mapping.annotations.ClusteringColumn;
import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;

import java.util.Date;

import static java.lang.Float.NaN;
import static java.lang.Float.isNaN;


/**
 * An AggregationRecord is a POJO mapping the table bbdata2.aggregation to java objects.
 * It also implements some methods to merge two windows or to update a window with a late record.
 * date: 10.03.17
 *
 * @author Lucy Linder <lucy.derlin@gmail.com>
 */
@Table(keyspace = "bbdata2", name = "aggregations",
        readConsistency = "QUORUM",
        writeConsistency = "QUORUM",
        caseSensitiveKeyspace = false,
        caseSensitiveTable = false)
public class AggregationRecord {

    @PartitionKey(0)
    @Column(name = "minutes")
    public int minutes;

    @PartitionKey(1)
    @Column(name = "object_id")
    public int objectId = -1;

    @PartitionKey(2)
    public String date;

    @ClusteringColumn
    public Date timestamp;

    @Column(name = "count")
    public int count = 0;

    @Column(name = "min")
    public float min = NaN;

    @Column(name = "max")
    public float max = NaN;

    @Column(name = "sum")
    public float sum = 0;

    @Column(name = "last")
    public float lastMeasure;

    @Column(name = "last_ts")
    public long lastMeasureTimestamp;

    @Column(name = "k")
    public float k = NaN;

    @Column(name = "k_sum")
    public float kSum = NaN;

    @Column(name = "k_sum_2")
    public float kSumSquared = NaN;

    @Column(name = "std")
    public float std = NaN;

    @Column(name = "mean")
    public float mean = NaN;


    // ----------------------------------------------------

    /**
     * add one late record to the current window
     * @param r the late record to add
     */
    public void addOne(AggregationRecord r) {
        assert r.count == 1;
        if (this.objectId != r.objectId) {
            throw new RuntimeException(String.format("trying to merge different aggregation records: %s | %s", this, r));
        }

        this.count += 1;
        this.sum += r.lastMeasure;
        this.min = Math.min(this.min, r.lastMeasure);
        this.max = Math.max(this.max, r.lastMeasure);
        if (this.lastMeasureTimestamp < r.lastMeasureTimestamp) {
            this.lastMeasureTimestamp = r.lastMeasureTimestamp;
            this.lastMeasure = r.lastMeasure;
        }

        if (!isNaN(k)) {
            kSum += r.lastMeasure - k;
            kSumSquared += (r.lastMeasure - k) * (r.lastMeasure - k);
            updateStdDev();
        }
        updateMean();
    }

    /**
     * add one measure to the current window.
     * @deprecated use {@link #addOne(AggregationRecord)} instead
     * @param m the measure to add
     */
    public void addOne(Measure m) {
        if (this.objectId != m.objectId) {
            throw new RuntimeException(String.format("trying to merge different aggregation records: %s | %s", this, m));
        }

        this.count += 1;
        this.sum += m.floatValue;
        this.min = Math.min(this.min, m.floatValue);
        this.max = Math.max(this.max, m.floatValue);
        if (this.lastMeasureTimestamp < m.timestamp.getTime()) {
            this.lastMeasureTimestamp = m.timestamp.getTime();
            this.lastMeasure = m.floatValue;
        }

        if (!isNaN(k)) {
            kSum += m.floatValue - k;
            kSumSquared += (m.floatValue - k) * (m.floatValue - k);
            updateStdDev();
        }
        updateMean();
    }

    // ----------------------------------------------------

    /**
     * compute the final standard deviation based on k and kSumSquared.
     * This should be called right before closing the window.
     */
    public void updateStdDev() {

        assert k != NaN && kSumSquared != NaN && count > 0;
        // divide by count if want to compute the exact variance of the given data
        // divide by (count-1) if data are samples of a larger population
        float var = (kSumSquared - (kSum * kSum) / count) / count;
        std = (float) Math.sqrt(var);
    }

    /**
     * compute the mean based on count and sum.
     * This should be called right before closing the window or after the count/sum has been modified (in case of a
     * late record for instance).
     */
    public void updateMean() {
        assert count != 0;
        mean = sum / count;
    }

    @Override
    public String toString() {
        return "AggregationRecord{" +
                "minutes=" + minutes +
                ", objectId=" + objectId +
                ", date='" + DateUtil.dateToString(timestamp) +
                ", timestamp=" + timestamp +
                ", count=" + count +
                '}';
    }

}
