package KMean;

import java.lang.Math;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.WritableComparable;

public class Element implements WritableComparable<Center>{
    
    /**
     * coordinata centro
     */
    protected DoubleWritable x;

    /**
     * coordinata centro
     */
    protected DoubleWritable y;

    /**
     * coordinata centro
     */
    protected DoubleWritable z;

    protected DoubleWritable instanceNum;
    
    public Element(){
        this.x = new DoubleWritable(0);
        this.y = new DoubleWritable(0);
        this.z = new DoubleWritable(0);
        this.instanceNum = new DoubleWritable(0);
    }

    public Element(double c1, double c2, double c3){
        this.x = new DoubleWritable(c1);
        this.y = new DoubleWritable(c2);
        this.z = new DoubleWritable(c3);
        this.instanceNum = new DoubleWritable(0);
    }

    public void readFields(DataInput dataInput) throws IOException {

        this.x = new DoubleWritable(dataInput.readDouble());
        this.y = new DoubleWritable(dataInput.readDouble());
        this.z = new DoubleWritable(dataInput.readDouble());
        this.instanceNum = new DoubleWritable(dataInput.readDouble());
    }

    public void write(DataOutput dataOutput) throws IOException {

        dataOutput.writeDouble(this.getX());
        dataOutput.writeDouble(this.getY());
        dataOutput.writeDouble(this.getZ());
        dataOutput.writeDouble(this.getInstance());

    }

    public int compareTo(Center p) {
        return 0;
    }

    public double getX(){
        return this.x.get();
    }

    public double getY(){
        return this.y.get();
    }

    public double getZ(){
        return this.z.get();
    }

    public void incInstance(){
        this.instanceNum.set(this.instanceNum.get() + 1);
    }

    public double getInstance(){
       return this.instanceNum.get();
    }

    public void addInstance(Center c){
        this.instanceNum.set(this.instanceNum.get() + c.getInstance());
    }

}