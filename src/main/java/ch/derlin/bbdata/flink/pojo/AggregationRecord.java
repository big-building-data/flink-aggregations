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
     *
     * @param r the late record to add
     */
    public void addOne(AggregationRecord r) {
        // this window should have record(s), the other should have only 1
        if (this.count == 0 || r.count > 1) {
            throw new RuntimeException(String.format(
                    "error in addOne. this.count (%d) == 0 || other.count (%d) > 1", this.count, r.count));
        }
        // both windows should have the same key
        if (this.objectId != r.objectId || this.minutes != r.minutes || !this.date.equals(r.date)) {
            throw new RuntimeException(String.format(
                    "trying to merge different aggregation records: %s | %s", this, r));
        }
        assert !Float.isNaN(k);

        boolean withStdev = !isNaN(k); // count > 0, hence this tells us if advanced or basic
        addValue(r.lastMeasure, r.lastMeasureTimestamp, withStdev);
        computeFinalValues();
    }

    // ----------------------------------------------------

    /**
     * update fields by adding the corresponding (value, timestamp)
     *
     * @param value     the float value of a measure
     * @param timestamp the timestamp (in millis) of a measure
     * @param withStdev whether or not to compute standard deviation
     */
    public void addValue(float value, long timestamp, boolean withStdev) {
        // min
        if (isNaN(min) || min > value) min = value;
        if (isNaN(max) || max < value) max = value;
        count += 1;
        sum += value;
        if (lastMeasureTimestamp < timestamp) {
            lastMeasure = value;
            lastMeasureTimestamp = timestamp;
        }

        if (withStdev) {
            if (count > 1 && isNaN(k))
                throw new RuntimeException("Mixing with/without stdev in the same window");
            updateK(value);
        }
    }

    /**
     * update the "final" values that require computation, such as
     * mean and standard deviation
     */
    public void computeFinalValues() {
        updateMean();
        if (!isNaN(k)) updateStdDev();
    }

    /**
     * update the intermediate values needed for the standard deviation.
     * this should be called on each new value.
     *
     * @param v the measure's value
     */
    protected void updateK(float v) {
        // stdev
        if (isNaN(k)) {
            k = v;
            kSum = kSumSquared = 0;
        } else {
            // incremental computation of the variance while avoiding catastrophic cancellation
            // see: https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Computing_shifted_data
            kSum += v - k;
            kSumSquared += (v - k) * (v - k);
        }
    }

    /**
     * compute the final standard deviation based on k and kSumSquared.
     * This should be called right before closing the window.
     */
    protected void updateStdDev() {
        assert !isNaN(k) && !isNaN(kSumSquared) && count > 0;
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
    protected void updateMean() {
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
