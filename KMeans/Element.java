package KMeans;

import java.lang.Math;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import java.util.ArrayList;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.WritableComparable;

public class Element implements WritableComparable<Center>{
    

    /**
    * Lista dei parametri degli elementi del dataset
    */
    protected ArrayList<DoubleWritable> parameters;

        /**
     * numero di elementi di un determinato centro
     */
    public DoubleWritable instanceNum;
    
    /**
     * Calcolo della distanza euclidea tra c1 e c2
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
     * Calcolo della distanza euclidea tra c1 e c2
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
     * Costruttore base
     */
    public Element(){
        parameters = new ArrayList<DoubleWritable>();
    }

    /**
     * Costruttore che inizializza un cero numero di parametri
     */
    public Element(int size){
        parameters = new ArrayList<DoubleWritable>();
        for(int i = 0 ; i < size; i++){
            parameters.add(new DoubleWritable(0));
        }

    }

    /**
     * Costruttore che inizializza i parametri
     */
    public Element(ArrayList<DoubleWritable> param){
        parameters = new ArrayList<DoubleWritable>();

        for(DoubleWritable d : param){
            parameters.add(d);
        }
    }

    public void readFields(DataInput dataInput) throws IOException {
        //leggo quanti parametri ha l'elemento
        int size = dataInput.readInt();
        parameters = new ArrayList<DoubleWritable>();
        //leggo i parametri
        for(int i = 0; i < size; i++){
            parameters.add(new DoubleWritable(dataInput.readDouble()));
        }
    }

    public void write(DataOutput dataOutput) throws IOException {
        //scrivo il numero di parametri
        dataOutput.writeInt(parameters.size());
        //scrivo i parametri
        for(int i = 0; i < parameters.size(); i++){
            dataOutput.writeDouble(parameters.get(i).get());
        }

    }

    public int compareTo(Center p) {
        return 0;
    }

    //Ritorna la lista dei parametri
    public ArrayList<DoubleWritable> getParam(){
        return parameters;
    }

    //Permette di aggiungere parametri
    public void addParam(double d){
        parameters.add(new DoubleWritable(d));
    }


}