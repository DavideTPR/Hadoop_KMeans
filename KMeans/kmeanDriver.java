
package KMean;


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

public class KMeanDriver {


	private static void createCenter(int k, int param, Configuration conf, Path input, Path centers, Vector<Integer> index, int maxNumber){
	
		try {
			SequenceFile.Writer centersFile = SequenceFile.createWriter(conf, SequenceFile.Writer.file(centers), SequenceFile.Writer.keyClass(IntWritable.class), SequenceFile.Writer.valueClass(Center.class));


			FileSystem fs = FileSystem.get(conf);
			FileStatus[] ri = fs.listStatus(input);

			//SequenceFile.Writer centersFile = new SequenceFile.Writer(fs, conf, centers, IntWritable.class, Center.class);

			/*for (int i = 0; i < ri.length; i++) {
				System.out.println(i + "-------------------------------------" + ri[i].getPath());
			}*/

			//Apro il dataset per estrapolare i centri iniziali 
			BufferedReader brData = new BufferedReader(new InputStreamReader(fs.open(ri[0].getPath())));
			String line;
			Center tmp=new Center();

			//Scelgo le prime k righe del dataset come centri
			if(index == null && maxNumber == 0){

				for(int i=0; i<k; i++){

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

				//brData.close();
				//centersFile.close();
			}
			else
				if (maxNumber == 0){
					int idx = 0;
					int id = 0;

					//se lindice attuale è quello nella lista dei centri allora salvo l'elemento come centro
					while(!index.isEmpty()){

						line = brData.readLine();

						if(index.contains(idx)){
							
							String[] data = line.split("\\t");

							//Leggo le righe del dataset e le riscrivo nel file sequenziale che verrà letto durante l'esecuzione del mapper
							tmp = new Center();
							for(int l = 0; l < param; l++){
								tmp.addParam(Double.parseDouble(data[l]));
							}
							System.out.println("-------------------" + tmp.toString());
							centersFile.append(new IntWritable(id), tmp);
							index.removeElement(idx);
							id++;
						}

						idx++;
					}

					//brData.close();
					//centersFile.close();
				}
				else{
					Vector<Integer> cent = new Vector<Integer>();
					int n = 0;
					int idx = 0;
					int id = 0;

					for(int i=0; i<k; i++){
						do {
							n = (int)(Math.random()*(maxNumber-1));
						} while (!cent.contains(n));
						cent.add(n);
					}


					while(!cent.isEmpty()){

						System.out.println("---ZZZZZZZZZZZZZZZZZZZZZZZZ   ");
						line = brData.readLine();
						System.out.println("---KKKKKKKKKKKKKKKKKKKKKKKK   ");
						if(cent.contains(idx)){
							System.out.println("---111111111111111111111111   ");
							String[] data = line.split("\\t");
							System.out.println("---222222222222222222222222   ");
							//Leggo le righe del dataset e le riscrivo nel file sequenziale che verrà letto durante l'esecuzione del mapper
							System.out.println("---333333333333333333333333   ");
							tmp = new Center();
							System.out.println("---444444444444444444444444   ");
							for(int l = 0; l < param; l++){
								System.out.println("+++++++++++++++++++++++AAAAA   " + l);
								tmp.addParam(Double.parseDouble(data[l]));
								System.out.println("+++++++++++++++++++++++BBBBB   " + l);
							}
							System.out.println("-------------------" + tmp.toString());
							System.out.println("---555555555555555555555555   ");
							centersFile.append(new IntWritable(id), tmp);
							System.out.println("---666666666666666666666666   ");
							cent.removeElement(idx);
							System.out.println("---777777777777777777777777   ");
							id++;
						}

						idx++;
					}

					//brData.close();
					//centersFile.close();


				}

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
			System.out.println("- USE:");
			System.out.println("$HADOOP_HOME/bin/hadoop jar KMeans.jar input_dir output_dir number_centers number_parameters loop max_line_num");
		}
		else{
			Configuration conf = new Configuration();

			Path centers = new Path("centers/cent.seq");
			Path centersTxt = new Path("centers/cent.txt");
			Path input = new Path(args[0]);
			Path output = new Path(args[1]);
			int numCenters = Integer.parseInt(args[2]);
			int numParams = Integer.parseInt(args[3]);
			int loop = Integer.parseInt(args[4]);
			int maxNum = Integer.parseInt(args[5]);

			Vector<Integer> index = new Vector<Integer>();
			index.add(0);
			index.add(71238);
			index.add(117707);
			index.add(165945);
			index.add(311672);


			conf.set("centersPath", centers.toString());
			conf.setInt("numParams", numParams);


			FileSystem fs = FileSystem.get(conf);

			JobClient my_client = new JobClient();

			createCenter(numCenters, numParams, conf, input, centers, null, maxNum);

			for(int n = 0; n < loop; n++){

				//conf.setInt("number", n);
			
				// Create a configuration object for the job
				Job job_conf = Job.getInstance(conf, "KMeans");
				job_conf.setJarByClass(KMeanDriver.class);

				// Specify data type of output key and value
				job_conf.setOutputKeyClass(IntWritable.class);
				job_conf.setOutputValueClass(Center.class);

				// Specify names of Mapper and Reducer Class
				job_conf.setMapperClass(KMeanMapper.class);
				job_conf.setCombinerClass(KMeanCombiner.class);
				job_conf.setReducerClass(KMeanReducer.class);

				//job_conf.setNumReduceTasks(1);

				// Specify formats of the data type of Input and output
				//job_conf.setOutputKeyClass(IntWritable.class);
				//job_conf.setOutputValueClass(Center.class);

				
				FileInputFormat.setInputPaths(job_conf, input);
				FileOutputFormat.setOutputPath(job_conf, output);
				
				job_conf.setMapOutputKeyClass(IntWritable.class);
				job_conf.setMapOutputValueClass(Center.class);

				job_conf.waitForCompletion(true);

				//elimino l'output per poter rieseguire
				fs.delete(output, true);
			}

			//rieseguo solo il mapper per avere come output il dataset con gli assegnamenti ai rispettivi centri
			Job job_conf = Job.getInstance(conf, "KMeans");
			job_conf.setJarByClass(KMeanDriver.class);

			job_conf.setMapperClass(KMeanMapper.class);

			FileInputFormat.setInputPaths(job_conf, input);
			FileOutputFormat.setOutputPath(job_conf, output);
			
			job_conf.setMapOutputKeyClass(IntWritable.class);
			job_conf.setMapOutputValueClass(Center.class);

			job_conf.waitForCompletion(true);


			//my_client.setConf(job_conf);
			try {
				// Run the job 
				//JobClient.runJob(job_conf);
				




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
