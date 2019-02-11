package KMean;

import java.io.IOException;
import java.util.*;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class KMeanCombiner extends Reducer<IntWritable, Center, IntWritable, Center> {

	
	public void reduce(IntWritable t_key, Iterator<Center> values, Context context)  throws IOException, InterruptedException {
		
		int key = t_key.get();

		Center valueSum = new Center();
		
		while (values.hasNext()) {
			// replace type of value with the actual type of our value
			Center v = (Center) values.next();
			valueSum.sumCenter(v);
			valueSum.incInstance();
			
		}
		context.write(new IntWritable(key), valueSum);
	}
}
