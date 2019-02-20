package KMeans;

import java.io.IOException;
import java.util.*;
import java.util.Iterator;
import java.util.ArrayList;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.DoubleWritable;

import org.apache.hadoop.fs.FSDataOutputStream;

public class KMeansCombiner extends Reducer<IntWritable, Center, IntWritable, Center> {

	//HashMap<IntWritable, Center> Centri = new HashMap<IntWritable, Center>();

	public void reduce(IntWritable key, Iterable<Center> values, Context context) throws IOException, InterruptedException {

		int iKey = key.get();

		Configuration conf = context.getConfiguration();
		int size = conf.getInt("numParams", 3);

		double count = 0;
		ArrayList<DoubleWritable> value = new ArrayList<DoubleWritable>();
		
        for(int i = 0 ; i < size; i++){
            value.add(new DoubleWritable(0));
        }

		//Center valueSum = new Center();
		for(Center c : values) {
			//Somma parziale dei parametri di tutti gli elemnti appartenenti ad uno stesso centro
			for(int i = 0; i < size; i++){
				value.get(i).set(value.get(i).get() + c.getParam().get(i).get());
			}

			//aumento il numero di elementi appartenenti a quel centro
			count++;
			
		}

		Center valueSum = new Center(value, count);

		context.write(key, valueSum);
	}



	/*protected void cleanup(Context context) throws IOException, InterruptedException{
		Configuration conf = context.getConfiguration();
		Path centers = new Path(conf.get("centersPath"));

		FileSystem fs = FileSystem.get(conf);
		int tmp = conf.getInt("number", 0);
		FSDataOutputStream fsdos = fs.create(new Path("centers/cent_"+ tmp +".txt"), true);

		SequenceFile.Writer centersFile = SequenceFile.createWriter(conf, SequenceFile.Writer.file(centers), SequenceFile.Writer.keyClass(IntWritable.class), SequenceFile.Writer.valueClass(Center.class));
		Set set = Centri.entrySet();
		Iterator newCenters = set.iterator();

		while(newCenters.hasNext()){
			Map.Entry cent = (Map.Entry)newCenters.next();
			centersFile.append(cent.getKey(), cent.getValue());
			fsdos.writeChars("-" + cent.getKey().toString() + " : " + cent.getValue().toString() + "\n");
		}
		fsdos.close();
		centersFile.close();
	}*/
}
