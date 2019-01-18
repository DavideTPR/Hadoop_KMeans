package KMean;

import java.io.IOException;
import java.util.Vector;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
//import org.apache.log4j.Logger;


//import Center;

public class KMeanMapper extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, Element> {
	//private final static IntWritable one = new IntWritable(1);

	//vettore dei centroidi
	private static Vector<Center> centroids = new Vector<Center>();
	//private Logger logger = Logger.getLogger(Map.class);

    protected void setup(org.apache.hadoop.mapreduce.Mapper.Context context) throws IOException, InterruptedException{

    	//configurazione del sistema
		Configuration conf = context.getConfiguration();//new Configuration();
		//Root principale del file system hdfs
		//System.out.println("fs.default.name : - " + conf.get("fs.defaultFS"));

		//Cartella in cui è presente il dataset da cui estrapolare i centroidi
		//String uri = conf.get("fs.defaultFS")+"/user/davide.tarasconi/kMeans";

		//ricerca, apertura e lettura dataset
		/*try {

			FileSystem fs = FileSystem.get(new Configuration());
			FileStatus[] ri = fs.listStatus(new Path(uri));

			for (int i = 0; i < ri.length; i++) {
				System.out.println(i + "-------------------------------------" + ri[i].getPath());
			}
		}catch (Exception e) {
			e.printStackTrace();
		}*/

		//TODO
		//lettura file e scelta n centri
		
		Path centers = new Path(conf.get("centersPath"));
		SequenceFile.Reader centRead = new SequenceFile.Reader(conf, SequenceFile.Reader.file(centers));
		IntWritable key = new IntWritable();
		Center cent = new Center();

		while(centRead.next(key, cent)){
			Center tmp = new Center(cent.getX(), cent.getY(), cent.getZ());
			//logger.fatal(tmp.toString());
			centroids.add(tmp);
		}

		centRead.close();
    }

	public void map(LongWritable key, Text value, OutputCollector<IntWritable, Element> output, Reporter reporter) throws IOException {

		double minDis = Double.MAX_VALUE;
		double dis;
		int index = -1;
		//Vector<double> instance;
		Element element;
		Center cent;
		IntWritable idx;

		String valueString = value.toString();
		//split string containing TAB
		String[] SingleData = valueString.split("\\t"); // or \\t

		//instance.add(double.parseDouble(SingleData[0]));
		//instance.add(double.parseDouble(SingleData[1]));
		//instance.add(double.parseDouble(SingleData[2]));

		element = new Center(Double.parseDouble(SingleData[0]), Double.parseDouble(SingleData[1]), Double.parseDouble(SingleData[2]));

		for(int i = 0; i < centroids.size(); i++){
			dis = Center.distance(centroids.get(i), element);
			if(dis < minDis)
			{
				cent = centroids.get(i);
				minDis = dis;
				index = i;
			}
		}

		idx = new IntWritable(index);

		/*String valueString = value.toString();
		String[] SingleCountryData = valueString.split(",");*/
		output.collect(idx, element);
	}
}
