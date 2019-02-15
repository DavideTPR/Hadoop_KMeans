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

import org.apache.hadoop.fs.FSDataOutputStream;

public class KMeanCombiner extends Reducer<IntWritable, Center, IntWritable, Center> {

	//HashMap<IntWritable, Center> Centri = new HashMap<IntWritable, Center>();

	public void reduce(IntWritable key, Iterable<Center> values, Context context) throws IOException, InterruptedException {

		int iKey = key.get();

		double count = 0;
		double x = 0;
		double y = 0;
		double z = 0;

		//Center valueSum = new Center();
		for(Center c : values) {
			// replace type of value with the actual type of our value
			//Center v = (Center) values.next();
			
			//valueSum.sumCenter(c);
			
			//valueSum.incInstance();

			x += c.getX();
			y += c.getY();
			z += c.getZ();


			count++;
			
		}

		Center valueSum = new Center(x, y, z, count);
		//Centri.put(new IntWritable(iKey),valueSum);

		context.write(key, valueSum);
	}



	/*protected void cleanup(Context context) throws IOException, InterruptedException{
		Configuration conf = context.getConfiguration();
		Path centers = new Path(conf.get("centersPath"));

		FileSystem fs = FileSystem.get(conf);
		int tmp = conf.getInt("number", 0);
		FSDataOutputStream fsdos = fs.create(new Path("centers/cent_"+ tmp +".txt"), true);

		SequenceFile.Writer centersFile = SequenceFile.createWriter(conf, SequenceFile.Writer.file(centers), SequenceFile.Writer.keyClass(IntWritable.class), SequenceFile.Writer.valueClass(Center.class));
		Set set = Centri.entrySet();
		Iterator newCenters = set.iterator();

		while(newCenters.hasNext()){
			Map.Entry cent = (Map.Entry)newCenters.next();
			centersFile.append(cent.getKey(), cent.getValue());
			fsdos.writeChars("-" + cent.getKey().toString() + " : " + cent.getValue().toString() + "\n");
		}
		fsdos.close();
		centersFile.close();
	}*/
}
