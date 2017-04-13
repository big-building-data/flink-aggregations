package ch.derlin.bbdata.flink.pojo;

import ch.derlin.bbdata.commons.dateutils.DateTimeFormatUTC;
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
@Table(keyspace = "bbdata2", name = "aggregations",
        readConsistency = "QUORUM",
        writeConsistency = "QUORUM",
        caseSensitiveKeyspace = false,
        caseSensitiveTable = false)
public class AggregationRecord {

    private static transient DateTimeFormatUTC dtf = new DateTimeFormatUTC();

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

    @Override
    public String toString() {
        return "AggregationRecord{" +
                "minutes=" + minutes +
                ", objectId=" + objectId +
                ", date='" + date + '\'' +
                ", timestamp=" + dtf.format(timestamp) +
                ", count=" + count +
                '}';
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

}
