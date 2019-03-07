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

/**
 * Combiner class used to get partial sum of data passed from Mapper. It counts number of instances belonging to 
 * analyzed key.
 * The output will be passed to Reducer process
 * 
 * @author Davide Tarasconi
 */

public class KMeansCombiner extends Reducer<IntWritable, Element, IntWritable, Element> {


	public void reduce(IntWritable key, Iterable<Element> values, Context context) throws IOException, InterruptedException {

		int iKey = key.get();

		Configuration conf = context.getConfiguration();
		int size = conf.getInt("numParams", 3);

		double count = 0;
		ArrayList<DoubleWritable> value = new ArrayList<DoubleWritable>();
		
        for(int i = 0 ; i < size; i++){
            value.add(new DoubleWritable(0));
        }

		for(Element c : values) {
			
			//Partial sum of parameters of every elements belonging to same centroid
			for(int i = 0; i < size; i++){
				value.get(i).set(value.get(i).get() + c.getParam().get(i).get());
			}

			//increment number of instances of that centroid
			count++;
			
		}

		//Create element and set number of instances and parameters
		Element valueSum = new Element(value, count);
		//pass value to Reducer
		context.write(key, valueSum);
	}
}
