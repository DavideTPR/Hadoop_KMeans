package KMean;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class SalesCountryReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {

	public void reduce(int t_key, Iterator<Center> values, OutputCollector<int,Center> output, Reporter reporter) throws IOException {
		int key = t_key;
		Center newCenter = new Center()
		while (values.hasNext()) {
			// replace type of value with the actual type of our value
			Center value = (Center) values.next();
			newCenter.sumCenter(value);
			newCenter.addInstance(value);
		}

		newCenter.mean();


		output.collect(key, newCenter);
	}
}
