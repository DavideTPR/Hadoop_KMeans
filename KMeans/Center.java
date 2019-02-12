package KMean;

import java.lang.Math;
import org.apache.hadoop.io.DoubleWritable;

public class Center extends Element{


    
    public static double distance(Center c1, Center c2) {
        return Math.sqrt(Math.pow(c1.getX() - c2.getX(), 2) + Math.pow(c1.getY() - c2.getY(), 2) + Math.pow(c1.getZ() - c2.getZ(), 2));
    }
    public static double distance(Center c1, Element c2) {
        return Math.sqrt(Math.pow((c1.getX() - c2.getX()), 2) + Math.pow((c1.getY() - c2.getY()), 2) + Math.pow((c1.getZ() - c2.getZ()), 2));
    }
    
    public Center(){
        super();
    }

    public Center(double c1, double c2, double c3){
        super(c1, c2, c3);
    }


    public void sumCenter(Center c){
        this.x.set(this.x.get() + c.x.get());
        this.y.set(this.y.get() + c.y.get());
        this.z.set(this.z.get() + c.z.get());
    }

    public void mean(){
        this.x.set(this.x.get() / this.getInstance());
        this.y.set(this.y.get() / this.getInstance());
        this.z.set(this.z.get() / this.getInstance());
    }

    public String toString(){
        return this.x.get() + " - " + this.y.get() + " - " + this.z.get() + " ---- " + this.getInstance();
    }

}