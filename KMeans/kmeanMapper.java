package KMean;

import java.io.IOException;
import java.util.Vector;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
//import org.apache.hadoop.mapreduce.Mapper.Context;
//import org.apache.log4j.Logger;


//import Center;

public class KMeanMapper extends Mapper<Object, Text, IntWritable, Center> {
	//private final static IntWritable one = new IntWritable(1);

	//vettore dei centroidi
	private static Vector<Center> centroids = new Vector<Center>();
	//private Logger logger = Logger.getLogger(Map.class);
	int index = -1;

    protected void setup(Context context) throws IOException, InterruptedException 	{
		index = 33;
    	//configurazione del sistema
		Configuration conf = context.getConfiguration();//new Configuration();
		
		//APRO IL FILE SEQUENZIALE CONTENENTE I CENTRI
		Path centers = new Path(conf.get("centersPath"));
		SequenceFile.Reader centRead = new SequenceFile.Reader(conf, SequenceFile.Reader.file(centers));
		IntWritable key = new IntWritable();
		Center cent = new Center();
		
		//leggo il file contenente i centri e inizializzo centroids
		while(centRead.next(key, cent)){
			Center tmp = new Center(cent.getX(), cent.getY(), cent.getZ());
			//logger.fatal(tmp.toString());
			centroids.add(tmp);
		}

		centRead.close();
    }

	public void mapmap(Object key, Text value, Context context)throws IOException,InterruptedException {

		double minDis = 1000000;
		double dis;
		//int index = -1;
		//Vector<double> instance;
		Center element;
		Center cent;
		IntWritable idx;

		String valueString = value.toString();
		//split string containing TAB
		String[] SingleData = valueString.split("\\t"); // or \\t



		element = new Center(Double.parseDouble(SingleData[0]), Double.parseDouble(SingleData[1]), Double.parseDouble(SingleData[2]));

		/*for(int i = 0; i < centroids.size(); i++){
			dis = Center.distance(centroids.get(i), element);
			if(dis < minDis)
			{
				cent = centroids.get(i);
				minDis = dis;
				index = i;
			}
		}*/

		int i = 0;
		//index=1;
		for(Center c : centroids){
			//index=2;
			dis = Center.distance(c, element);
			//index=3;
			if(dis < minDis)
			{
				cent = c;
				minDis = dis;
				index = i;
			}
			i++;
			index=4;
		}

		idx = new IntWritable(index);

		context.write(idx, element);
	}
}
