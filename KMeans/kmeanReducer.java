package KMean;

import java.io.IOException;
import java.util.*;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class KMeanReducer extends MapReduceBase implements Reducer<IntWritable, Center, IntWritable, Center> {


	public void reduce(IntWritable t_key, Iterator<Center> values, OutputCollector<IntWritable,Center> output, Reporter reporter) throws IOException {
		int key = t_key.get();
		Center newCenter = new Center();
		while (values.hasNext()) {
			// replace type of value with the actual type of our value
			Center value = (Center) values.next();
			newCenter.sumCenter(value);
			newCenter.addInstance(value);
		}

		newCenter.mean();


		output.collect(new IntWritable(key), newCenter);
	}
}
