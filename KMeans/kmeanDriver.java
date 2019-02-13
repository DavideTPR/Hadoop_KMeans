
package KMean;


import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.FileReader;

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
//import org.apache.hadoop.fs.FileStatus;
//import org.apache.hadoop.conf.Configuration;

public class KMeanDriver {


	private static void createCenter(int k, Configuration conf, Path input, Path centers){
	
		try {
			SequenceFile.Writer centersFile = SequenceFile.createWriter(conf, SequenceFile.Writer.file(centers), SequenceFile.Writer.keyClass(IntWritable.class), SequenceFile.Writer.valueClass(Center.class));


			FileSystem fs = FileSystem.get(conf);
			FileStatus[] ri = fs.listStatus(input);

			//SequenceFile.Writer centersFile = new SequenceFile.Writer(fs, conf, centers, IntWritable.class, Center.class);

			/*for (int i = 0; i < ri.length; i++) {
				System.out.println(i + "-------------------------------------" + ri[i].getPath());
			}*/

			//Apro il dataset per estrapolare i centri iniziali, in questo caso le prima k righe 
			BufferedReader brData = new BufferedReader(new InputStreamReader(fs.open(ri[0].getPath())));
			String line;
			Center tmp=new Center();

			for(int i=0; i<k; i++)
			{
				line = brData.readLine();
				String[] data = line.split("\\t");

				//Leggo le righe del dataset e le riscrivo nel file sequenziale che verrà letto durante l'esecuzione del mapper
				tmp = new Center(Double.parseDouble(data[0]), Double.parseDouble(data[1]), Double.parseDouble(data[2]));
				System.out.println("-------------------" + tmp.toString());
				centersFile.append(new IntWritable(i), tmp);
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

		//TODO
		Configuration conf = new Configuration();

		Path centers = new Path("centers/cent.seq");
		Path input = new Path(args[0]);
		Path output = new Path(args[1]);
		conf.set("centersPath", centers.toString());


		FileSystem fs = FileSystem.get(conf);

		JobClient my_client = new JobClient();

		createCenter(5, conf, input, centers);

		for(int n = 0; n < 10; n++){
		
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

			// Specify formats of the data type of Input and output
			job_conf.setOutputKeyClass(IntWritable.class);
			job_conf.setOutputValueClass(Center.class);

			
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





			SequenceFile.Reader centRead = new SequenceFile.Reader(conf, SequenceFile.Reader.file(centers));
			IntWritable key = new IntWritable();
			Center cent = new Center();
			Center tmp1;
			System.out.println("++++++++++++++++++++++");
			while(centRead.next(key, cent)){
				tmp1 = new Center(cent.getX(), cent.getY(), cent.getZ());
				System.out.println("----------"+key.toString()+"---------" + tmp1.toString());
				//centroids.add(tmp);
			}



		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
