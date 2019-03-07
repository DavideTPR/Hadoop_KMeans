package KMeans;

import java.lang.Math;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.DoubleWritable;

/**
 * @deprecated
 * It inherits from Element for differentiating normal element from centroids
 * 
 * @author Davide Tarasconi
 */

public class Center extends Element{

    /**
     * Number of element of a specified centrods
     */
    public DoubleWritable instanceNum;
    
    /**
     * Euclidean distance between c1 and c2
     */
    public static double distance(Center c1, Center c2) {
        double res = 0;
        //Math.pow((c1.getX() - c2.getX()), 2)
        for(int i = 0;  i < c1.getParam().size(); i++){
            res += Math.pow((c1.getParam().get(i).get() - c2.getParam().get(i).get()), 2);
        }

        return Math.sqrt(res);
    }

    /**
     * Euclidean distance between c1 and c2
     */
    public static double distance(Center c1, Element c2) {
        double res = 0;
        //Math.pow((c1.getX() - c2.getX()), 2)
        for(int i = 0;  i < c1.getParam().size(); i++){
            res += Math.pow((c1.getParam().get(i).get() - c2.getParam().get(i).get()), 2);
        }

        return Math.sqrt(res);
    }
    
    /**
     * Basic constructor
     */
    public Center(){
        super();
        this.instanceNum = new DoubleWritable(1);
    }

    /**
     * Set parameters
     */
    public Center(ArrayList<DoubleWritable> param){
        super(param);
        this.instanceNum = new DoubleWritable(1);
    }

    /**
     * Set parameters and number of instances
     */
    public Center(ArrayList<DoubleWritable> param, double iNum){
        super(param);
        this.instanceNum = new DoubleWritable(iNum);
    }


    /*public void sumCenter(Center c){
        this.x.set(this.x.get() + c.x.get());
        this.y.set(this.y.get() + c.y.get());
        this.z.set(this.z.get() + c.z.get());
        //incInstance();
    }*/

    /**
     * Computation of the mean
     */
    public void mean(){
        for(int i = 0; i < parameters.size(); i++){
            parameters.get(i).set(parameters.get(i).get() / this.getInstance());
        }
        this.instanceNum.set(1);
    }

    public String toString(){
        String s = "";
        for(int i = 0; i < parameters.size(); i++){
            s += parameters.get(i).get();
            if(i < parameters.size()-1){
                s += " - ";
            }
        }

        //s += " ---- " + this.getInstance();
        return s;
    }

    public void incInstance(){
        double tmp = this.instanceNum.get();
        this.instanceNum.set(tmp + 1);
    }

    public double getInstance(){
       return instanceNum.get();
    }

    public void setInstance(double n){
        this.instanceNum = new DoubleWritable(n);
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