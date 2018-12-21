package KMean;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;

public class SalesMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
	//private final static IntWritable one = new IntWritable(1);

	//vettore dei centroidi
	private static Vector<Vector<double>> centroids;

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

	public void map(LongWritable key, Text value, OutputCollector<int, Vector<double>> output, Reporter reporter) throws IOException {


		double minDis = double.MAX_VALUE;
		double dis;
		int index = -1;
		Vector<double> instance;

		String valueString = value.toString();
		//split string containing TAB
		String[] SingleData = valueString.split("\\t"); // or \\t

		instance.add(double.parseDouble(SingleData[0]));
		instance.add(double.parseDouble(SingleData[1]));
		instance.add(double.parseDouble(SingleData[2]));

		for(int i = 0; i < centroids.size(); i++){
			dis = Math.sqrt(Math.pow(instance.get(0) - centroids.get(i).get(0), 2) + Math.pow(instance.get(1) - centroids.get(i).get(1), 2) + Math.pow(instance.get(2) - centroids.get(i).get(2), 2));
			if(dis < minDis)
			{
				minDis = dis;
				index = i;
			}
		}


		/*String valueString = value.toString();
		String[] SingleCountryData = valueString.split(",");*/
		output.collect(index, instance);
	}
}
