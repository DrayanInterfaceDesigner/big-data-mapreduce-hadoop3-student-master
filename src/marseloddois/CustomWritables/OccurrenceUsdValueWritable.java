package marseloddois.CustomWritables;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class OccurrenceUsdValueWritable implements WritableComparable<OccurrenceUsdValueWritable> {
    private int occurrences;
    private float usdValue;

    public OccurrenceUsdValueWritable() {
    }

    public OccurrenceUsdValueWritable(float usdValue, int occurrences) {
        this.occurrences = occurrences;
        this.usdValue = usdValue;
    }

    public int getOccurrences() {
        return occurrences;
    }

    public float getUsdValue() {
        return usdValue;
    }

    public void setOccurrences(int occurrences) {
        this.occurrences = occurrences;
    }

    public void setUsdValue(float usdValue) {
        this.usdValue = usdValue;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        OccurrenceUsdValueWritable that = (OccurrenceUsdValueWritable) o;
        return Float.compare(that.occurrences, this.occurrences) == 0 && usdValue == that.usdValue;
    }

    @Override
    public int hashCode() {
        return Objects.hash(occurrences, usdValue);
    }

    @Override
    public String toString() {
        return super.toString();
    }

    @Override
    public int compareTo(OccurrenceUsdValueWritable o) {
        if(this.hashCode() < o.hashCode()){
            return -1;
        }else if(this.hashCode() > o.hashCode()){
            return +1;
        }
        return 0;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(occurrences);
        dataOutput.writeFloat(usdValue);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        occurrences = dataInput.readInt();
        usdValue = dataInput.readFloat();
    }
}

