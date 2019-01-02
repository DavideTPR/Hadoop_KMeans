package KMean;

import java.lang.Math;

public class Element{
    
    /**
     * coordinata centro
     */
    protected double x;

    /**
     * coordinata centro
     */
    protected double y;

    /**
     * coordinata centro
     */
    protected double z;
    
    public Element(){
        this.x = 0;
        this.y = 0;
        this.z = 0;
    }

    public Element(double c1, double c2, double c3){
        this.x = c1;
        this.y = c2;
        this.z = c3;
    }

}