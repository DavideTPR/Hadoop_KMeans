package KMean;

import java.lang.Math;
import org.apache.hadoop.io.DoubleWritable;

public class Center extends Element{

    /**
     * number if instance of a center
     */
    private double instanceNum;
    
    public static double distance(Center c1, Center c2) {
        return Math.sqrt(Math.pow(c1.x.get() - c2.x.get(), 2) + Math.pow(c1.y.get() - c2.y.get(), 2) + Math.pow(c1.z.get() - c2.z.get(), 2));
    }
    public static double distance(Center c1, Element c2) {
        return Math.sqrt(Math.pow(c1.x.get() - c2.x.get(), 2) + Math.pow(c1.y.get() - c2.y.get(), 2) + Math.pow(c1.z.get() - c2.z.get(), 2));
    }
    
    public Center(){
        super();
        this.instanceNum = 0;
    }

    public Center(double c1, double c2, double c3){
        super(c1, c2, c3);
        this.instanceNum = 0;
    }


    public void sumCenter(Center c){
        this.x.set(c.x.get());
        this.y.set(c.y.get());
        this.z.set(c.z.get());
    }

    public void incInstance(){
        this.instanceNum++;
    }

    public void addInstance(Center c){
        this.instanceNum += c.instanceNum;
    }

    public void mean(){
        this.x.set(this.x.get() / this.instanceNum);
        this.y.set(this.y.get() / this.instanceNum);
        this.z.set(this.z.get() / this.instanceNum);
    }

}