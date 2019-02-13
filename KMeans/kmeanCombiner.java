package KMean;

import java.io.IOException;
import java.util.*;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.fs.FileSystem;

public class KMeanCombiner extends Reducer<IntWritable, Center, IntWritable, Center> {

	HashMap<IntWritable, Center> Centri = new HashMap<IntWritable, Center>();

	public void reduce(IntWritable key, Iterable<Center> values, Context context) throws IOException, InterruptedException {

		int iKey = key.get();

		Center valueSum = new Center();
		for(Center c : values) {
			// replace type of value with the actual type of our value
			//Center v = (Center) values.next();
			valueSum.sumCenter(c);
			valueSum.incInstance();
			
		}

		//valueSum.setInstance(l);
		Centri.put(new IntWritable(iKey),valueSum);

		context.write(key, valueSum);
	}



	protected void cleanup(Context context) throws IOException, InterruptedException{
		Configuration conf = context.getConfiguration();
		Path centers = new Path(conf.get("centersPath"));
		SequenceFile.Writer centersFile = SequenceFile.createWriter(conf, SequenceFile.Writer.file(centers), SequenceFile.Writer.keyClass(IntWritable.class), SequenceFile.Writer.valueClass(Center.class));
		Set set = Centri.entrySet();
		Iterator newCenters = set.iterator();

		while(newCenters.hasNext()){
			Map.Entry cent = (Map.Entry)newCenters.next();
			centersFile.append(cent.getKey(), cent.getValue());
		}
		centersFile.close();
	}
}
