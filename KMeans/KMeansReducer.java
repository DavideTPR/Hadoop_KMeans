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

public class KMeansReducer extends Reducer<IntWritable, Center, IntWritable, Center> {

	//Per salvare il centro calcolato e poterlo scrivere nel file sequenziale
	HashMap<IntWritable, Center> Centri = new HashMap<IntWritable, Center>();


	public void reduce(IntWritable key, Iterable<Center> values, Context context) throws IOException, InterruptedException {

		int iKey = key.get();
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
		Center newCenter = new Center(value, count);
		newCenter.mean();

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
