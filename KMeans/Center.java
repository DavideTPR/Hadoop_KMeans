package KMean;

import java.lang.Math;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;

public class Center extends Element{

    public DoubleWritable instanceNum;
    
    public static double distance(Center c1, Center c2) {
        return Math.sqrt(Math.pow(c1.getX() - c2.getX(), 2) + Math.pow(c1.getY() - c2.getY(), 2) + Math.pow(c1.getZ() - c2.getZ(), 2));
    }
    public static double distance(Center c1, Element c2) {
        return Math.sqrt(Math.pow((c1.getX() - c2.getX()), 2) + Math.pow((c1.getY() - c2.getY()), 2) + Math.pow((c1.getZ() - c2.getZ()), 2));
    }
    
    public Center(){
        super();
        this.instanceNum = new DoubleWritable(1);
    }

    public Center(double c1, double c2, double c3){
        super(c1, c2, c3);
        this.instanceNum = new DoubleWritable(1);
    }


    public void sumCenter(Center c){
        this.x.set(this.x.get() + c.x.get());
        this.y.set(this.y.get() + c.y.get());
        this.z.set(this.z.get() + c.z.get());
        incInstance();
    }

    public void mean(){
        this.x.set(this.x.get() / this.getInstance());
        this.y.set(this.y.get() / this.getInstance());
        this.z.set(this.z.get() / this.getInstance());
        this.instanceNum.set(1);
    }

    public String toString(){
        return this.x.get() + " - " + this.y.get() + " - " + this.z.get()/* + " ---- " + this.getInstance()*/;
    }

    public void incInstance(){
        double tmp = this.instanceNum.get();
        this.instanceNum.set(tmp + 1);
    }

    public double getInstance(){
       return instanceNum.get();
    }

    public void addInstance(Center c){
        this.instanceNum.set(this.instanceNum.get() + c.getInstance());
    }

    public void readFields(DataInput dataInput) throws IOException {

        super.readFields(dataInput);
        this.instanceNum = new DoubleWritable(dataInput.readDouble());
    }

    public void write(DataOutput dataOutput) throws IOException {

        super.write(dataOutput);
        dataOutput.writeDouble(this.getInstance());

    }

    public int compareTo(Center p) {
        return 0;
    }

}