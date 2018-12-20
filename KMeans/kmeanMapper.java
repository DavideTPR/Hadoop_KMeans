package KMean;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;

public class SalesMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
	private final static IntWritable one = new IntWritable(1);

	//vettore dei centroidi
	private static Vector<double> centroids;

    protected void setup(org.apache.hadoop.mapreduce.Mapper.Context context) throws IOException, InterruptedException{

    	//configurazione del sistema
		Configuration conf = new Configuration();
		//Root principale del file system hdfs
		//System.out.println("fs.default.name : - " + conf.get("fs.defaultFS"));

		//Cartella in cui è presente il dataset da cui estrapolare i centroidi
		String uri = conf.get("fs.defaultFS")+"/user/davide.tarasconi/kMeans";

		//ricerca, apertura e lettura dataset
		try {

			FileSystem fs = FileSystem.get(new Configuration());
			FileStatus[] ri = fs.listStatus(new Path(uri));

			for (int i = 0; i < ri.length; i++) {
				System.out.println(i + "-------------------------------------" + ri[i].getPath());
			}
		}catch (Exception e) {
			e.printStackTrace();
		}

		//TODO
		//lettura file e scelta n centri

    }

	public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {


		double minDis = double.MAX_VALUE;
		double dis;
		int index = -1;

		for(int i = 0; i < centroids.size(); i++){

		}


		/*String valueString = value.toString();
		String[] SingleCountryData = valueString.split(",");*/
		output.collect(index, value);
	}
}
