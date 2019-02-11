package KMean;

import java.io.IOException;
import java.util.*;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class KMeanReducer extends Reducer<IntWritable, Center, IntWritable, Center> {

	
	public void reduce(IntWritable t_key, Iterator<Center> values, Context context)  throws IOException, InterruptedException {
		int key = t_key.get();
		Center newCenter = new Center();
		while (values.hasNext()) {
			// replace type of value with the actual type of our value
			Center value = (Center) values.next();
			newCenter.sumCenter(value);
			newCenter.addInstance(value);
		}

		newCenter.mean();


		context.write(new IntWritable(key), newCenter);
	}
}
