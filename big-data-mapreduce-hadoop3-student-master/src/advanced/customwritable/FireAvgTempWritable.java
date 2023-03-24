package advanced.customwritable;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.checkerframework.checker.units.qual.Temperature;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class FireAvgTempWritable implements WritableComparable<FireAvgTempWritable>{

    // Java BEAN
    // tem atributos privados

    float somaTemperatura;
    int n;

    // tem construtor vazio

    public FireAvgTempWritable() {}

    public FireAvgTempWritable(float somaTemperatura, int n) {
        this.somaTemperatura = somaTemperatura;
        this.n = n;
    }

    // gets e sets

    public float getSomaTemperatura() {
        return somaTemperatura;
    }

    public void setSomaTemperatura(float somaTemperatura) {
        this.somaTemperatura = somaTemperatura;
    }

    public int getN() {
        return n;
    }

    public void setN(int n) {
        this.n = n;
    }

    @Override
    public int compareTo(FireAvgTempWritable o) {
        if(this.hashCode() < o.hashCode()) return -1;
        else if(this.hashCode() > o.hashCode()) return +1;
        return 0;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FireAvgTempWritable that = (FireAvgTempWritable) o;
        return Float.compare(that.somaTemperatura, somaTemperatura) == 0 && n == that.n;
    }

    @Override
    public int hashCode() {
        return Objects.hash(somaTemperatura, n);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeFloat(somaTemperatura);
        dataOutput.writeInt(n);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        // USAR A MESMA ORDEM DO WRITE
        somaTemperatura = dataInput.readFloat();
        n = dataInput.readInt();
    }
}
