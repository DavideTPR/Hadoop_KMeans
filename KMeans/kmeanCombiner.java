package KMean;

import java.io.IOException;
import java.util.*;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class KMeanCombiner extends MapReduceBase implements Reducer<Integer, Element, Integer, Center> {

	
	public void reduce(Integer t_key, Iterator<Element> values, OutputCollector<Integer,Center> output, Reporter reporter) throws IOException {
		
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
