package KMean;

import java.lang.Math;

public class Center extends Element{

    /**
     * number if instance of a center
     */
    private double instanceNum;
    
    public static double distance(Center c1, Center c2) {
        return Math.sqrt(Math.pow(c1.x - c2.x, 2) + Math.pow(c1.y - c2.y, 2) + Math.pow(c1.z - c2.z, 2));
    }
    public static double distance(Center c1, Element c2) {
        return Math.sqrt(Math.pow(c1.x - c2.x, 2) + Math.pow(c1.y - c2.y, 2) + Math.pow(c1.z - c2.z, 2));
    }
    
    public Center(){
        super();
        this.instanceNum = 0;
    }

    public Center(double c1, double c2, double c3){
        super(c1, c2, c3);
        this.instanceNum = 0;
    }


    public sumCenter(Center c){
        this.x = c.x;
        this.y = c.y;
        this.z = c.z;
    }

    public incInstance(){
        this.instanceNum++;
    }

    public addInstance(Center c){
        this.instanceNum += c.instanceNum;
    }

    public mean(){
        this.x /= this.instanceNum;
        this.y /= this.instanceNum;
        this.z /= this.instanceNum;
    }

}