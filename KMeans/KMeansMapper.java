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


public class KMeansMapper extends Mapper<Object, Text, IntWritable, Element> {

	//vettore dei centroidi
	private static Vector<Element> centroids = new Vector<Element>();

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		
    //configurazione del sistema
		Configuration conf = context.getConfiguration();//new Configuration();
		
		//APRO IL FILE SEQUENZIALE CONTENENTE I CENTRI
		Path centers = new Path(conf.get("centersPath"));
		SequenceFile.Reader centRead = new SequenceFile.Reader(conf, SequenceFile.Reader.file(centers));
		IntWritable key = new IntWritable();
		Element cent = new Element();
		
		//leggo il file contenente i centri e inizializzo centroids
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
		Element element;	//utilizziamo Center al posto di Element perchè l'output del mapper deve coincidere con l'input del combiner che a sua volda 
							//deve coincidere col suo output perchè non è conosciuto il numero di volte in cui verrà applicato il Combiner 
		Element cent;
		IntWritable idx;

		Configuration conf = context.getConfiguration();
		int size = conf.getInt("numParams", 3);
		String split = conf.get("split");
		String valueString = value.toString();

		//split della stringa in base al valore passato come parametro
		String[] SingleData = valueString.split(split); // or \\t

		//creo e inizializzo l'elemento in base a ciò che è stato letto nel dataset
		element= new Element();
		for(int n = 0; n < size; n++){
			element.addParam(Double.parseDouble(SingleData[n]));
		}

		int i = 0;
		
		for(Element c : centroids){
			
			//calcolo la distanza da tutti i centri e lo assegno a quello più vicino
			dis = Element.distance(c, element);
			
			//controllo se la distanza è la minore
			if(dis < minDis)
			{
				cent = c;
				minDis = dis;
				index = i;
			}
			i++;
		}

		idx = new IntWritable(index);
		//passo i valori al Combiner
		context.write(idx, element);
	}
}
