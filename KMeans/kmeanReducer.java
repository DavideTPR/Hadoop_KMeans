package KMean;

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


import org.apache.hadoop.fs.FSDataOutputStream;
import java.io.FileOutputStream;

public class KMeanReducer extends Reducer<IntWritable, Center, IntWritable, Center> {

	HashMap<IntWritable, Center> Centri = new HashMap<IntWritable, Center>();


	public void reduce(IntWritable key, Iterable<Center> values, Context context) throws IOException, InterruptedException {
		int iKey = key.get();
		Center newCenter = new Center();
		for(Center c : values) {
			// replace type of value with the actual type of our value
			//Center value = (Center) values.next();
			newCenter.sumCenter(c);
			newCenter.addInstance(c);
		}

		newCenter.mean();

		Centri.put(new IntWritable(iKey), newCenter);

		context.write(new IntWritable(iKey), newCenter);
	}

	//@Override
	protected void cleanup(Context context) throws IOException, InterruptedException{

		Configuration conf = context.getConfiguration();
		Path centers = new Path(conf.get("centersPath"));
		FileSystem fs = FileSystem.get(conf);
        fs.delete(centers, true);

		//int tmp = conf.getInt("number", 0);
		//FSDataOutputStream fsdos = fs.create(new Path("centers/cent_"+ tmp +".txt"), true);

		SequenceFile.Writer centersFile = SequenceFile.createWriter(conf, SequenceFile.Writer.file(centers), SequenceFile.Writer.keyClass(IntWritable.class), SequenceFile.Writer.valueClass(Center.class));
		
		Set set = Centri.entrySet();
		Iterator newCenters = set.iterator();

		while(newCenters.hasNext()){
			Map.Entry cent = (Map.Entry)newCenters.next();
			centersFile.append(cent.getKey(), cent.getValue());
			//fsdos.writeChars("-" + cent.getKey().toString() + " : " + cent.getValue().toString() + "\n");
		}
		//fsdos.close();
		centersFile.close();
	
	}
}
