package KMean;

import java.lang.Math;

public class Center{
    
    /**
     * coordinata centro
     */
    private double x;

    /**
     * coordinata centro
     */
    private double y;

    /**
     * coordinata centro
     */
    private double z;
    
    public static double distance(Center c1, Center c2) {
        return Math.sqrt(Math.pow(c1.x - c2.x, 2) + Math.pow(c1.y - c2.y, 2) + Math.pow(c1.z - c2.z, 2));
    }
    
    public Center(){

    }

    public Center(double c1, double c2, double c3){
        this.x = c1;
        this.y = c2;
        this.z = c3;
    }

}