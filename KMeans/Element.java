package KMeans;

import java.lang.Math;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import java.util.ArrayList;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Classe per la gestione degli elementi letti dal datacenter durante le operazioni di Map-Reduce
 * che ne permette anche la gestione come centri greazie alla possibilita di inserire il numero di elementi
 * appartenenti ad un determinato centro e alla possibilità di calcolare la media
 * 
 * 
 * @author Davide Tarasconi
 */

public class Element implements WritableComparable<Element>{
    

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
    /*public static double distance(Center c1, Center c2) {
        double res = 0;
        //Math.pow((c1.getX() - c2.getX()), 2)
        for(int i = 0;  i < c1.getParam().size(); i++){
            res += Math.pow((c1.getParam().get(i).get() - c2.getParam().get(i).get()), 2);
        }

        return Math.sqrt(res);
    }*/

    /**
     * Calcolo della distanza euclidea tra c1 e c2
     * @param c1 elemento 1
     * @param c2 elemento 2
     * @return distanza tra c1 e c2
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
     * Costruttore base
     */
    public Element(){
        parameters = new ArrayList<DoubleWritable>();
        this.instanceNum = new DoubleWritable(1);
    }

    /**
     * Costruttore che inizializza un cero numero di parametri
     * @param size numero di parametri dell'elemento
     */
    public Element(int size){
        parameters = new ArrayList<DoubleWritable>();
        for(int i = 0 ; i < size; i++){
            parameters.add(new DoubleWritable(0));
        }

        this.instanceNum = new DoubleWritable(1);

    }

    /**
     * Costruttore che inizializza i parametri
     * @param param parametri dell'elemento
     */
    public Element(ArrayList<DoubleWritable> param){
        parameters = new ArrayList<DoubleWritable>();

        for(DoubleWritable d : param){
            parameters.add(d);
        }

        this.instanceNum = new DoubleWritable(1);
    }

    /**
     * Costruttore che inizializza i parametri e il conteggio delle istanze
     * @param param paramentri dell'elemento
     * @param iNum numero delle istanze (Nel caso l'elemento sia un centro è il numero di istanze appartenenti al cluser del centro)
     */
    public Element(ArrayList<DoubleWritable> param, double iNum){
        
        parameters = new ArrayList<DoubleWritable>();

        for(DoubleWritable d : param){
            parameters.add(d);
        }

        this.instanceNum = new DoubleWritable(iNum);
    }

    public void readFields(DataInput dataInput) throws IOException {
        //leggo quanti parametri ha l'elemento
        int size = dataInput.readInt();
        parameters = new ArrayList<DoubleWritable>();
        //leggo i parametri
        for(int i = 0; i < size; i++){
            parameters.add(new DoubleWritable(dataInput.readDouble()));
        }

        this.instanceNum = new DoubleWritable(dataInput.readDouble());
    }

    public void write(DataOutput dataOutput) throws IOException {
        //scrivo il numero di parametri
        dataOutput.writeInt(parameters.size());
        //scrivo i parametri
        for(int i = 0; i < parameters.size(); i++){
            dataOutput.writeDouble(parameters.get(i).get());
        }

        dataOutput.writeDouble(this.getInstance());

    }

    public int compareTo(Element p) {
        return 0;
    }

    /** 
     * @return lista dei parametri
    */
    public ArrayList<DoubleWritable> getParam(){
        return parameters;
    }

    /**
     * Permette di aggiungere parametri
     * @param d nuovo parametro
     */
    public void addParam(double d){
        parameters.add(new DoubleWritable(d));
    }

    /**
     * Calcola la media dei parametri
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
     * @deprecated Incrementa il numero delle istanze (mai usato per problemi)
     */
    public void incInstance(){
        double tmp = this.instanceNum.get();
        this.instanceNum.set(tmp + 1);
    }
    /**
     * @return numero delle istanze
     */
    public double getInstance(){
       return instanceNum.get();
    }

    /**
     * Inizializza il numero delle istanze
     * @param n numero delle istanze
     */
    public void setInstance(double n){
        this.instanceNum = new DoubleWritable(n);
     }

     /**
      * @deprecated Somma le istanze dell'oggetto con quelle dell'oggetto passato come parametro
      * @param c elemento di cui dovremo sommare le istanze
      */
    public void addInstance(Element c){
        this.instanceNum.set(this.instanceNum.get() + c.getInstance());
    }

}