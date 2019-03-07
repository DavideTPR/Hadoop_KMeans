package KMeans;

import java.io.IOException;
import java.util.Vector;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.DoubleWritable;

/**
 * Mapper Class that reads centroids from sequential file during setup phase, then execute Map process for every dataset's
 * record and it assigns it to the nearest centroids.
 * The output will be passed to Combiner process
 * 
 * @author Davide Tarasconi 
 */


public class KMeansMapper extends Mapper<Object, Text, IntWritable, Element> {

	//Centroids vector
	private static Vector<Element> centroids = new Vector<Element>();

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		
    //system configuration
		Configuration conf = context.getConfiguration();//new Configuration();
		
		//Open sequence file and read the centroids
		Path centers = new Path(conf.get("centersPath"));
		SequenceFile.Reader centRead = new SequenceFile.Reader(conf, SequenceFile.Reader.file(centers));
		IntWritable key = new IntWritable();
		Element cent = new Element();
		
		//read the file and set centroids
		while(centRead.next(key, cent)){
			Element tmp = new Element(cent.getParam());
			centroids.add(tmp);
		}

		centRead.close();
	}

		
	public void map(Object key, Text value, Context context)throws IOException,InterruptedException {

		double minDis = 1000000;
		double dis;
		int index = -1;
		//Vector<double> instance;
		Element element;	//We use Element class instead of Center class because mapper output must coincide with combiner input
											//that will coincide with its output because we don't know how many times Combiner will be execute
		Element cent;
		IntWritable idx;

		Configuration conf = context.getConfiguration();
		int size = conf.getInt("numParams", 3);
		String split = conf.get("split");
		String valueString = value.toString();

		//split using separator
		String[] SingleData = valueString.split(split); // or \\t

		//Create and set element read into the dataset
		element= new Element();
		for(int n = 0; n < size; n++){
			element.addParam(Double.parseDouble(SingleData[n]));
		}

		int i = 0;
		
		for(Element c : centroids){
			
			//computation of the distance
			dis = Element.distance(c, element);
			
			//check if it is the lower distance
			if(dis < minDis)
			{
				cent = c;
				minDis = dis;
				index = i;
			}
			i++;
		}

		idx = new IntWritable(index);
		//pass value to Combiner
		context.write(idx, element);
	}
}
