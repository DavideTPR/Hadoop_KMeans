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


/**
 * Reducer class used to get the total number of instances for a certain key (centroid) and compute new centroids
 * computing the mean.
 * During setup phase it loads old centroids from sequence file, they will be compared with the new ones to check convergence
 * less then a threshold.
 * During cleanup phase it overwrites sequence file with new centroids that will be used into following loop.
 * 
 * 
 * @author Davide Tarasconi
 */

public class KMeansReducer extends Reducer<IntWritable, Element, IntWritable, Element> {

	/**
	 * Map to save computer centrod to write into sequence file
	 */
	HashMap<IntWritable, Element> Centri = new HashMap<IntWritable, Element>();

	/**
	 * Centroids vector
	 */
	private static Vector<Element> OldCenter = new Vector<Element>();

	/**
	 * Counter used to check convergence
	 */
	public enum CONVERGENCE{
		CONVERGE
	}


	@Override
	protected void setup(Context context) throws IOException, InterruptedException 	{
		
		//system configuration
		Configuration conf = context.getConfiguration();//new Configuration();
		
		//Open sequence file and read the centroids
		Path centers = new Path(conf.get("centersPath"));
		SequenceFile.Reader centRead = new SequenceFile.Reader(conf, SequenceFile.Reader.file(centers));
		IntWritable key = new IntWritable();
		Element cent = new Element();
		
		//read the file and set centroids
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

		//number of instances
		double count = 0;
		//element that will contain sum of combiner's partial sum
		ArrayList<DoubleWritable> value = new ArrayList<DoubleWritable>();
		//set value
        for(int i = 0 ; i < size; i++){
            value.add(new DoubleWritable(0));
        }

		for(Element c : values) {
			//Partial sum of parameters of every elements belonging to same centroid
			for(int i = 0; i < size; i++){
				value.get(i).set(value.get(i).get() + c.getParam().get(i).get());
			}
			
			//increment number of instances of that centroid
			count += c.instanceNum.get();

		}

		//Find new centroid computing the mean
		Element newCenter = new Element(value, count);
		newCenter.mean();

		//Result desn't converge if distance between old and new centroids is greater than threshold
		if(Element.distance(OldCenter.get(key.get()), newCenter) > 0.01)
		{
			context.getCounter(CONVERGENCE.CONVERGE).increment(1);
		}

		Centri.put(new IntWritable(iKey), newCenter);

		context.write(new IntWritable(iKey), newCenter);
	}

	//@Override
	protected void cleanup(Context context) throws IOException, InterruptedException{

		//Update sequence file with centroids to use in following execution
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
