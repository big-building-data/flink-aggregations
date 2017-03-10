package ch.derlin.bbdata.flink.pojo;

import com.datastax.driver.mapping.annotations.ClusteringColumn;
import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;

import java.util.Date;
import java.util.Objects;

import static java.lang.Float.NaN;
import static java.lang.Float.isNaN;


/**
 * date: 10.03.17
 *
 * @author Lucy Linder <lucy.derlin@gmail.com>
 */
@Table(keyspace = "test_llinder", name = "aggregations",
        readConsistency = "QUORUM",
        writeConsistency = "QUORUM",
        caseSensitiveKeyspace = false,
        caseSensitiveTable = false)
public class AggregationRecord {

    @PartitionKey(0)
    @Column(name = "object_id")
    public int objectId = -1;

    @PartitionKey(1)
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
    public float kSum = 0;

    @Column(name = "k_sum_2")
    public float kSumSquared = 0;

    @Column(name = "std")
    public float std;

    @Column(name = "mean")
    public float mean;


    // ----------------------------------------------------

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

    public void updateStdDev() {

        assert k != NaN && kSumSquared != NaN && count > 0;
        // divide by count if want to compute the exact variance of the given data
        // divide by (count-1) if data are samples of a larger population
        float var = (kSumSquared - (kSum * kSum) / count) / count;
        std = (float) Math.sqrt(var);
    }

    public void updateMean() {
        assert count != 0;
        mean = sum / count;
    }


    // ----------------------------------------------------

    private AggregationRecord mergeTest(AggregationRecord other) {
        if (this.objectId != other.objectId ||
                !Objects.equals(this.date, other.date) ||
                !this.timestamp.equals(other.timestamp)) {
            throw new RuntimeException(String.format("trying to merge different aggregation records: %s | %s", this, other));
        }
        //assert (this.count > 1 && other.count > 1);

        float newMean = (this.count * this.mean + other.count * other.mean) / (this.count + other.count);
        if (!isNaN(k)) {
            this.std = this.count * ((this.std * this.std) + (this.mean - newMean)) +
                    other.count * ((other.std * other.std) + (other.mean - newMean));
        }
        this.mean = newMean;

        mergeBasicFields(other);
        return this;
    }

    private void merge2(AggregationRecord other) {
        assert (other.count > 1);

    }

    private void mergeBasicFields(AggregationRecord other) {
        this.count += other.count;
        this.sum += other.sum;
        this.min = Math.min(this.min, other.min);
        this.max = Math.max(this.max, other.max);
        if (this.lastMeasureTimestamp < other.lastMeasureTimestamp) {
            this.lastMeasureTimestamp = other.lastMeasureTimestamp;
            this.lastMeasure = other.lastMeasure;
        }
    }

    // ----------------------------------- getter/setter (for the mapper only)


    public int getObjectId() {
        return objectId;
    }

    public void setObjectId(int objectId) {
        this.objectId = objectId;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public Date getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Date timestamp) {
        this.timestamp = timestamp;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public float getMin() {
        return min;
    }

    public void setMin(float min) {
        this.min = min;
    }

    public float getMax() {
        return max;
    }

    public void setMax(float max) {
        this.max = max;
    }

    public float getSum() {
        return sum;
    }

    public void setSum(float sum) {
        this.sum = sum;
    }

    public long getLastMeasureTimestamp() {
        return lastMeasureTimestamp;
    }

    public void setLastMeasureTimestamp(long lastMeasureTimestamp) {
        this.lastMeasureTimestamp = lastMeasureTimestamp;
    }

    public float getStd() {
        return std;
    }

    public void setStd(float std) {
        this.std = std;
    }

    public float getMean() {
        return mean;
    }

    public void setMean(float mean) {
        this.mean = mean;
    }

    public float getLastMeasure() {
        return lastMeasure;
    }

    public void setLastMeasure(float lastMeasure) {
        this.lastMeasure = lastMeasure;
    }

    public float getK() {
        return k;
    }

    public void setK(float k) {
        this.k = k;
    }

    public float getkSum() {
        return kSum;
    }

    public void setkSum(float kSum) {
        this.kSum = kSum;
    }

    public float getkSumSquared() {
        return kSumSquared;
    }

    public void setkSumSquared(float kSumSquared) {
        this.kSumSquared = kSumSquared;
    }

}
