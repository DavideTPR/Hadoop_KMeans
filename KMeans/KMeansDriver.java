
package KMeans;


import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.File;
import java.util.Vector;
import java.lang.Math;
import java.util.ArrayList;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.FSDataOutputStream;
//import org.apache.hadoop.fs.FileStatus;
//import org.apache.hadoop.conf.Configuration;

public class KMeansDriver {


	private static void createCenter(int k, int param, Configuration conf, Path input, Path centers, int maxNumber){
	
		try {
			SequenceFile.Writer centersFile = SequenceFile.createWriter(conf, SequenceFile.Writer.file(centers), SequenceFile.Writer.keyClass(IntWritable.class), SequenceFile.Writer.valueClass(Center.class));


			FileSystem fs = FileSystem.get(conf);
			FileStatus[] ri = fs.listStatus(input);

			//Apro il dataset per estrapolare i centri iniziali 
			BufferedReader brData = new BufferedReader(new InputStreamReader(fs.open(ri[0].getPath())));
			String line;
			Center tmp=new Center();

			//Scelta dei centri in base alla infromazioni fornite alla funzione
			if(maxNumber == 0){
				//Scelgo le prime k righe del dataset come centri
				for(int i=0; i<k; i++){

					//leggo una riga e la suddivido in base al carattere
					line = brData.readLine();
					String[] data = line.split("\\t");

					//Leggo le righe del dataset e le riscrivo nel file sequenziale che verrà letto durante l'esecuzione del mapper
					//tmp = new Center(Double.parseDouble(data[0]), Double.parseDouble(data[1]), Double.parseDouble(data[2]));
					tmp = new Center();
					for(int l = 0; l < param; l++){
						tmp.addParam(Double.parseDouble(data[l]));
					}
					System.out.println("-------------------" + tmp.toString());
					centersFile.append(new IntWritable(i), tmp);
				}
			}
			else{
				Vector<Integer> cent = new Vector<Integer>();
				int n = 0;
				int idx = 0;
				int id = 0;

				//creo k numeri casuali diversi tra loro
				for(int i=0; i<k; i++){
					do {
						n = (int)(Math.random()*(maxNumber-1));
					} while (cent.contains(n));
					cent.add(n);
				}

				//aggiungo i centri confrontando l'indice attuale con quello scelto
				//mi fermo quando ho selezionato tutti i centri
				while(!cent.isEmpty()){

					line = brData.readLine();
					
					if(cent.contains(idx)){
						
						String[] data = line.split("\\t");
						
						//Leggo le righe del dataset e le riscrivo nel file sequenziale che verrà letto durante l'esecuzione del mapper
						tmp = new Center();
						for(int l = 0; l < param; l++){
							tmp.addParam(Double.parseDouble(data[l]));
						}
						System.out.println("-------------------" + tmp.toString());
						centersFile.append(new IntWritable(id), tmp);
						//rimuovo il centro perchè selezionato
						cent.removeElement(idx);
						
						id++;
					}

					idx++;
				}
			}

			//chiudo il sequence file e il file del dataset
			brData.close();
			centersFile.close();



			/*System.out.println("------------------- L E T T U R A    F I L E S E Q -------------------" + Double.MAX_VALUE +"-------------");





			SequenceFile.Reader centRead = new SequenceFile.Reader(conf, SequenceFile.Reader.file(centers));
			IntWritable key = new IntWritable();
			Center cent = new Center();
			Center tmp1;
			while(centRead.next(key, cent)){
				tmp1 = new Center(cent.getX(), cent.getY(), cent.getZ());
				Center tmpS = new Center(cent.getX(), cent.getY(), cent.getZ());
				tmpS.sumCenter(tmp);
				tmpS.incInstance();
				tmpS.incInstance();
				System.out.println("-------------------" + tmp1.toString());
				System.out.println("------------------- SUM ----------" + tmpS.toString());
				tmpS.mean();
				System.out.println("------------------- SUM/MED ----------" + tmpS.toString());
				System.out.println("------------------- DIST----------" + Center.distance(tmp, tmp1));
				//centroids.add(tmp);
			}*/





		}catch (Exception e) {
			e.printStackTrace();
		}


		
	
	}



	public static void main(String[] args) throws Exception {

		if(args.length != 6){
			//controllo per mostrare l'uso del programma
			System.out.println("- USE:");
			System.out.println("$HADOOP_HOME/bin/hadoop jar KMeans.jar input_dir output_dir number_centers number_parameters loop max_line_num");
		}
		else{
			Configuration conf = new Configuration();

			//file sequenziale centri
			Path centers = new Path("centers/cent.seq");
			//file di testo dei centri finali
			Path centersTxt = new Path("centers/cent.txt");
			//percorso in cui è situato l'input
			Path input = new Path(args[0]);
			//percorso in cui è situato l'output
			Path output = new Path(args[1]);
			//numeri di centri che si vogliono usare
			int numCenters = Integer.parseInt(args[2]);
			//numero di parametri da usare durante il calcolo
			int numParams = Integer.parseInt(args[3]);
			//numero di cicli per eseguire il calcolo
			int loop = Integer.parseInt(args[4]);
			//valore massimo per la scelta casuale dei centri
			int maxNum = Integer.parseInt(args[5]);
			//convergenza dei centri
			boolean converge = false;

			if(loop != 0){
				converge = true;
			}

			//imposto i parametri nella configurazione per usarli nel Mapper, nel Reducer e nel Combiner
			conf.set("centersPath", centers.toString());
			conf.setInt("numParams", numParams);


			FileSystem fs = FileSystem.get(conf);

			JobClient my_client = new JobClient();

			//creo i centri
			createCenter(numCenters, numParams, conf, input, centers, maxNum);
			int n = 0;
			//for(int n = 0; n < loop; n++){
			//while(!converge){
			while((!converge) ||  (n < loop)){

				//conf.setInt("number", n);
			
				// Create a configuration object for the job
				Job job_conf = Job.getInstance(conf, "KMeans");
				job_conf.setJarByClass(KMeansDriver.class);

				// Specify data type of output key and value
				job_conf.setOutputKeyClass(IntWritable.class);
				job_conf.setOutputValueClass(Center.class);

				// Specify names of Mapper, Combiner and Reducer Class
				job_conf.setMapperClass(KMeansMapper.class);
				job_conf.setCombinerClass(KMeansCombiner.class);
				job_conf.setReducerClass(KMeansReducer.class);

				//job_conf.setNumReduceTasks(1);
				
				//imposto i percorsi di input e output
				FileInputFormat.setInputPaths(job_conf, input);
				FileOutputFormat.setOutputPath(job_conf, output);
				
				//imposto i valori di uscita del mapper
				job_conf.setMapOutputKeyClass(IntWritable.class);
				job_conf.setMapOutputValueClass(Center.class);

				//eseguo il job
				job_conf.waitForCompletion(true);

				if(job_conf.getCounters().findCounter(KMeansReducer.CONVERGENCE.CONVERGE).getValue() == 0)
				{
					converge = true;
				}

				//elimino l'output per poter rieseguire
				fs.delete(output, true);
				n++;
			}

			//rieseguo solo il mapper per avere come output il dataset con gli assegnamenti ai rispettivi centri
			Job job_conf = Job.getInstance(conf, "KMeans");
			job_conf.setJarByClass(KMeansDriver.class);

			job_conf.setMapperClass(KMeansMapper.class);

			FileInputFormat.setInputPaths(job_conf, input);
			FileOutputFormat.setOutputPath(job_conf, output);
			
			job_conf.setMapOutputKeyClass(IntWritable.class);
			job_conf.setMapOutputValueClass(Center.class);

			job_conf.waitForCompletion(true);


			try {
				
				//Stampo il risultato dei centri e creo un file di testo contenente il risultato leggibile
				System.out.println("------------------- L E T T U R A    F I L E S E Q  AGGIORNATO-------------------");

				
				//FileOutputStream fos = new FileOutputStream(/*new File(*/"centers/cent.txt"/*)*/, false);

				FSDataOutputStream fsdos = fs.create(centersTxt, true);

				SequenceFile.Reader centRead = new SequenceFile.Reader(conf, SequenceFile.Reader.file(centers));
				IntWritable key = new IntWritable();
				Center cent = new Center();
				Center tmp1;
				System.out.println("++++++++++++++++++++++");
				while(centRead.next(key, cent)){
					tmp1 = new Center(cent.getParam());
					System.out.println("----------"+key.toString()+"---------" + tmp1.toString());
					fsdos.writeChars("-" + key.toString() + " : " + tmp1.toString() + "\n");
					//centroids.add(tmp);
				}

				fsdos.close();
				centRead.close();



			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
}
