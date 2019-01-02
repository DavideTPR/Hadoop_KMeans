package KMean;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class SalesCountryReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {

	public void reduce(int t_key, Iterable<Element> values, OutputCollector<Text,Center> output, Reporter reporter) throws IOException {
		
		int key = t_key;

		Center valueSum = new Center();
		
		while (values.hasNext()) {
			// replace type of value with the actual type of our value
			Center v = (Center) values.next();
			valueSum.sumCenter(v);
			valueSum.incInstance();
			
		}
		output.collect(key, valueSum);
	}
}
