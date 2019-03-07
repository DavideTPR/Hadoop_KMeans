package KMeans;

import java.lang.Math;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import java.util.ArrayList;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Class used to manage the record of the dataset during the elaboration. It also allows to manage centroids
 * setting number of parameters, computing the mean and memorizing number of insyances belonging to a specified
 * centroids
 * 
 * 
 * @author Davide Tarasconi
 */

public class Element implements WritableComparable<Element>{
    

    /**
    * List of dataset parameters
    */
    protected ArrayList<DoubleWritable> parameters;

    /**
    * Number of element of a specified centrods
    */
    public DoubleWritable instanceNum;
    
    /**
     * Euclidean distance between c1 and c2
     */
    /*public static double distance(Center c1, Center c2) {
        double res = 0;
        //Math.pow((c1.getX() - c2.getX()), 2)
        for(int i = 0;  i < c1.getParam().size(); i++){
            res += Math.pow((c1.getParam().get(i).get() - c2.getParam().get(i).get()), 2);
        }

        return Math.sqrt(res);
    }*/

    /**
     * Euclidean distance between c1 and c2
     * @param c1 element 1
     * @param c2 element 2
     * @return distance between c1 and c2
     */
    public static double distance(Element c1, Element c2) {
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
    public Element(){
        parameters = new ArrayList<DoubleWritable>();
        this.instanceNum = new DoubleWritable(1);
    }

    /**
     * Initialize some parameters
     * @param size number of parameters
     */
    public Element(int size){
        parameters = new ArrayList<DoubleWritable>();
        for(int i = 0 ; i < size; i++){
            parameters.add(new DoubleWritable(0));
        }

        this.instanceNum = new DoubleWritable(1);

    }

    /**
     * Initialize parameters
     * @param param parameters
     */
    public Element(ArrayList<DoubleWritable> param){
        parameters = new ArrayList<DoubleWritable>();

        for(DoubleWritable d : param){
            parameters.add(d);
        }

        this.instanceNum = new DoubleWritable(1);
    }

    /**
     * Initialize parameters and number of instances
     * @param param parameters
     * @param iNum number of instances (if it is a centroids it is the number of instances belonging to that centroids)
     */
    public Element(ArrayList<DoubleWritable> param, double iNum){
        
        parameters = new ArrayList<DoubleWritable>();

        for(DoubleWritable d : param){
            parameters.add(d);
        }

        this.instanceNum = new DoubleWritable(iNum);
    }

    public void readFields(DataInput dataInput) throws IOException {
        //read how many parameters we have
        int size = dataInput.readInt();
        parameters = new ArrayList<DoubleWritable>();
        //read parameters
        for(int i = 0; i < size; i++){
            parameters.add(new DoubleWritable(dataInput.readDouble()));
        }

        this.instanceNum = new DoubleWritable(dataInput.readDouble());
    }

    public void write(DataOutput dataOutput) throws IOException {
        //write number of parameters
        dataOutput.writeInt(parameters.size());
        //write parameters
        for(int i = 0; i < parameters.size(); i++){
            dataOutput.writeDouble(parameters.get(i).get());
        }

        dataOutput.writeDouble(this.getInstance());

    }

    public int compareTo(Element p) {
        return 0;
    }

    /** 
     * @return list of parameters
    */
    public ArrayList<DoubleWritable> getParam(){
        return parameters;
    }

    /**
     * Add new parameters
     * @param d new parameter
     */
    public void addParam(double d){
        parameters.add(new DoubleWritable(d));
    }

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

    /**
     * @deprecated Increment number of instances (never used)
     */
    public void incInstance(){
        double tmp = this.instanceNum.get();
        this.instanceNum.set(tmp + 1);
    }
    /**
     * @return Number of instances
     */
    public double getInstance(){
       return instanceNum.get();
    }

    /**
     * Set number of instances
     * @param n number of instances
     */
    public void setInstance(double n){
        this.instanceNum = new DoubleWritable(n);
     }

     /**
      * @deprecated Sum my number of instances with instances of c 
      * @param c element with certain number of instances
      */
    public void addInstance(Element c){
        this.instanceNum.set(this.instanceNum.get() + c.getInstance());
    }

}