package KMean;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class SalesCountryReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {

	public void reduce(int t_key, Vector<double> values, OutputCollector<Text,IntWritable> output, Reporter reporter) throws IOException {
		
		int key = t_key;

		Vector<double> valueSum = new Vector(4);
		valueSum.get(0) = 0;
		valueSum.get(1) = 0);
		valueSum.get(2) = 0;
		valueSum.get(3) = 0;
		
		while (values.hasNext()) {
			// replace type of value with the actual type of our value
			Vector<double> v = (Vector<double>) values.next();
			valueSum.get(0) += v.get(0);
			valueSum.get(1) += v.get(1);
			valueSum.get(2) += v.get(2);
			valueSum.get(3)++;
			
		}
		output.collect(key, valueSum);
	}
}
