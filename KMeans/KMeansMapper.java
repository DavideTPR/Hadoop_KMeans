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


//import Center;

public class KMeansMapper extends Mapper<Object, Text, IntWritable, Center> {

	//vettore dei centroidi
	private static Vector<Center> centroids = new Vector<Center>();

	@Override
    protected void setup(Context context) throws IOException, InterruptedException 	{
		
    //configurazione del sistema
		Configuration conf = context.getConfiguration();//new Configuration();
		
		//APRO IL FILE SEQUENZIALE CONTENENTE I CENTRI
		Path centers = new Path(conf.get("centersPath"));
		SequenceFile.Reader centRead = new SequenceFile.Reader(conf, SequenceFile.Reader.file(centers));
		IntWritable key = new IntWritable();
		Center cent = new Center();
		
		//leggo il file contenente i centri e inizializzo centroids
		while(centRead.next(key, cent)){
			Center tmp = new Center(cent.getParam());
			centroids.add(tmp);
		}

		centRead.close();
    }

		
	public void map(Object key, Text value, Context context)throws IOException,InterruptedException {

		double minDis = 1000000;
		double dis;
		int index = -1;
		//Vector<double> instance;
		Center element;	//utilizziamo Center al posto di Element perchè l'output del mapper deve coincidere con l'input del combiner che a sua volda 
										//deve coincidere col suo output perchè non è conosciuto il numero di volte in cui verrà applicato il Combiner 
		Center cent;
		IntWritable idx;

		Configuration conf = context.getConfiguration();
		int size = conf.getInt("numParams", 3);
		String valueString = value.toString();
		//split string containing TAB
		String[] SingleData = valueString.split("\\t"); // or \\t

		element= new Center();
		for(int n = 0; n < size; n++){
			element.addParam(Double.parseDouble(SingleData[n]));
		}

		int i = 0;
		
		for(Center c : centroids){
			
			//calcolo la distanza da tutti i centri e lo assegno a quello più vicino
			dis = Center.distance(c, element);
			
			if(dis < minDis)
			{
				cent = c;
				minDis = dis;
				index = i;
			}
			i++;
		}

		idx = new IntWritable(index);

		context.write(idx, element);
	}
}
