package KMean;

import java.io.IOException;
import java.util.*;
import java.util.Iterator;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.DoubleWritable;


import org.apache.hadoop.fs.FSDataOutputStream;
import java.io.FileOutputStream;

<<<<<<< HEAD:KMeans/KMeansReducer.java
public class KMeansReducer extends Reducer<Center, Center, IntWritable, Center> {
=======
public class KMeanReducer extends Reducer<IntWritable, Center, IntWritable, Center> {
>>>>>>> parent of 5adde21... V_3.5:KMeans/kmeanReducer.java

	//Per salvare il centro calcolato e poterlo scrivere nel file sequenziale
	HashMap<IntWritable, Center> Centri = new HashMap<IntWritable, Center>();

	public enum convergence{
		conv
	}
	
	//vettore dei centroidi
	/*private static HashMap<IntWritable, Center> centroids = new HashMap<IntWritable, Center>();

	@Override
    protected void setup(Context context) throws IOException, InterruptedException 	{
		
    //configurazione del sistema
		Configuration conf = context.getConfiguration();//new Configuration();
		
		//APRO IL FILE SEQUENZIALE CONTENENTE I CENTRI
		Path centers = new Path(conf.get("centersPath"));
		SequenceFile.Reader centRead = new SequenceFile.Reader(conf, SequenceFile.Reader.file(centers));
		IntWritable key = new IntWritable();
		Center cent = new Center();
		
		//leggo il file contenente i centri e inizializzo centroids
		while(centRead.next(key, cent)){
			Center tmp = new Center(cent.getParam());
			centroids.put(key, tmp);
		}

		centRead.close();
    }*/



	public void reduce(Center key, Iterable<Center> values, Context context) throws IOException, InterruptedException {

		//int iKey = key.get();
		Configuration conf = context.getConfiguration();
		int size = conf.getInt("numParams", 3);

		double count = 0;
		ArrayList<DoubleWritable> value = new ArrayList<DoubleWritable>();
        for(int i = 0 ; i < size; i++){
            value.add(new DoubleWritable(0));
        }

		for(Center c : values) {
			//Somma parziale dei parametri di tutti gli elemnti appartenenti ad uno stesso centro
			for(int i = 0; i < size; i++){
				value.get(i).set(value.get(i).get() + c.getParam().get(i).get());
			}
			
			//aumento il numero di elementi appartenenti a quel centro
			count += c.instanceNum.get();

		}

		//calcolo la media per trovare il nuovo centro
		Center newCenter = new Center(value, count, key.getIndex());
		newCenter.mean();

		if(Center.distance(key, newCenter) > 0.3){
			context.getCounter(convergence.conv).increment(1);
		}

		Centri.put(new IntWritable(key.getIndex()), newCenter);

		context.write(new IntWritable(key.getIndex()), newCenter);
	}

	//@Override
	protected void cleanup(Context context) throws IOException, InterruptedException{

		//Aggiorno il file sequenziale dei centri per l'esecuzione successiva

		Configuration conf = context.getConfiguration();
		Path centers = new Path(conf.get("centersPath"));
		FileSystem fs = FileSystem.get(conf);
        fs.delete(centers, true);

		SequenceFile.Writer centersFile = SequenceFile.createWriter(conf, SequenceFile.Writer.file(centers), SequenceFile.Writer.keyClass(IntWritable.class), SequenceFile.Writer.valueClass(Center.class));
		
		Set set = Centri.entrySet();
		Iterator newCenters = set.iterator();

		while(newCenters.hasNext()){
			Map.Entry cent = (Map.Entry)newCenters.next();
			centersFile.append(cent.getKey(), cent.getValue());
		}
		
		centersFile.close();
	
	}
}
