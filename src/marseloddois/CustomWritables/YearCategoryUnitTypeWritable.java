package marseloddois.CustomWritables;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class YearCategoryUnitTypeWritable implements WritableComparable<YearCategoryUnitTypeWritable> {
    private int year;
    private String category;
    private String unitType;

    public YearCategoryUnitTypeWritable() {
    }

    public YearCategoryUnitTypeWritable(int year, String category, String unitType) {
        this.year = year;
        this.category = category;
        this.unitType = unitType;
    }

    public int getYear() {
        return year;
    }

    public void setYear(int year) {
        this.year = year;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        YearCategoryUnitTypeWritable that = (YearCategoryUnitTypeWritable) o;
        return Integer.compare(that.year, this.year) == 0 &&
                this.category.equalsIgnoreCase(that.category) &&
                this.unitType.equalsIgnoreCase(that.unitType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(year, category, unitType);
    }

    @Override
    public String toString() {
        return super.toString();
    }

    @Override
    public int compareTo(YearCategoryUnitTypeWritable o) {
        if(this.hashCode() < o.hashCode()){
            return -1;
        }else if(this.hashCode() > o.hashCode()){
            return +1;
        }
        return 0;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(year);
        dataOutput.writeUTF(category);
        dataOutput.writeUTF(unitType);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        year = dataInput.readInt();
        category = dataInput.readUTF();
        unitType = dataInput.readUTF();
    }

    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    public String getUnitType() {
        return unitType;
    }

    public void setUnitType(String unitType) {
        this.unitType = unitType;
    }
}

