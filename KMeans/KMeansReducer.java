package KMeans;

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

public class KMeansReducer extends Reducer<IntWritable, Element, IntWritable, Element> {

	/**
	 * Mappa per salvare il centro calcolato e poterlo scrivere nel file sequenziale
	 */
	HashMap<IntWritable, Element> Centri = new HashMap<IntWritable, Element>();

	/**
	 * Vettore dei centroidi
	 */
	private static Vector<Element> OldCenter = new Vector<Element>();

	/**
	 * Utilizzato per valutare la convergenza
	 */
	public enum CONVERGENCE{
		CONVERGE
	}


	@Override
	protected void setup(Context context) throws IOException, InterruptedException 	{
		
		//configurazione del sistema
		Configuration conf = context.getConfiguration();//new Configuration();
		
		//APRO IL FILE SEQUENZIALE CONTENENTE I CENTRI
		Path centers = new Path(conf.get("centersPath"));
		SequenceFile.Reader centRead = new SequenceFile.Reader(conf, SequenceFile.Reader.file(centers));
		IntWritable key = new IntWritable();
		Element cent = new Element();
		
		//leggo il file contenente i centri e inizializzo centroids
		while(centRead.next(key, cent)){
			Element tmp = new Element(cent.getParam());
			OldCenter.add(tmp);
		}

		centRead.close();
	}


	public void reduce(IntWritable key, Iterable<Element> values, Context context) throws IOException, InterruptedException {

		int iKey = key.get();
		Configuration conf = context.getConfiguration();
		int size = conf.getInt("numParams", 3);

		//numero istanze
		double count = 0;
		//elemento che conterrà la somma degli oggetti del combiner e permetterà di calcolare il nuovo centro
		ArrayList<DoubleWritable> value = new ArrayList<DoubleWritable>();
		//inizializzo value
        for(int i = 0 ; i < size; i++){
            value.add(new DoubleWritable(0));
        }

		for(Element c : values) {
			//Somma parziale dei parametri di tutti gli elemnti appartenenti ad uno stesso centro
			for(int i = 0; i < size; i++){
				value.get(i).set(value.get(i).get() + c.getParam().get(i).get());
			}
			
			//aumento il numero di elementi appartenenti a quel centro
			count += c.instanceNum.get();

		}

		//calcolo la media per trovare il nuovo centro
		Element newCenter = new Element(value, count);
		newCenter.mean();

		//se la distnaza tra il veccio centro e quello nuovo è maggiore della soglia allora non converge
		if(Element.distance(OldCenter.get(key.get()), newCenter) > 0.01)
		{
			context.getCounter(CONVERGENCE.CONVERGE).increment(1);
		}

		Centri.put(new IntWritable(iKey), newCenter);

		context.write(new IntWritable(iKey), newCenter);
	}

	//@Override
	protected void cleanup(Context context) throws IOException, InterruptedException{

		//Aggiorno il file sequenziale dei centri per l'esecuzione successiva
		Configuration conf = context.getConfiguration();
		Path centers = new Path(conf.get("centersPath"));
		FileSystem fs = FileSystem.get(conf);
        fs.delete(centers, true);

		SequenceFile.Writer centersFile = SequenceFile.createWriter(conf, SequenceFile.Writer.file(centers), SequenceFile.Writer.keyClass(IntWritable.class), SequenceFile.Writer.valueClass(Element.class));
		
		Set set = Centri.entrySet();
		Iterator newCenters = set.iterator();

		while(newCenters.hasNext()){
			Map.Entry cent = (Map.Entry)newCenters.next();
			centersFile.append(cent.getKey(), cent.getValue());
		}
		
		centersFile.close();
	
	}
}
