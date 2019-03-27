
package KMeans;


import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.File;
import java.util.Vector;
import java.lang.Math;
import java.util.ArrayList;
import java.util.Date;

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


/**
 * Main class used for managing Map-Combine-Reduce processes and for creating starting centroids.
 * It traduces sequential files in readable files
 * 
 * @author Davide Tarasconi
 */


public class KMeansDriver {


	/**
	 * Method used to initialize starting centroids and to create sequential file
	 * @param k number of centroids
	 * @param param number of the parameters of the centroids
	 * @param conf system configuration
	 * @param input patho of the dataset
	 * @param centers path to save sequential file
	 * @param maxNumber max number within we will serch k centroids (0 for first k elements)
	 * @param split separator used into the dataset ('t' for tab)
	 */
	private static void createCenter(int k, int param, Configuration conf, Path input, Path centers, int maxNumber, String split){
	
		try {
			SequenceFile.Writer centersFile = SequenceFile.createWriter(conf, SequenceFile.Writer.file(centers), SequenceFile.Writer.keyClass(IntWritable.class), SequenceFile.Writer.valueClass(Element.class));


			FileSystem fs = FileSystem.get(conf);
			FileStatus[] ri = fs.listStatus(input);

			//I open the dataset for extracting starting centroids
			BufferedReader brData = new BufferedReader(new InputStreamReader(fs.open(ri[0].getPath())));
			String line;
			Element tmp=new Element();


			//Select centroids according to given information
			if(maxNumber == 0){
				//Select of the first k rows
				for(int i=0; i<k; i++){

					//read and split a row using separator
					line = brData.readLine();
					String[] data = line.split(split);


					//read dataset's rows and traduce it into a sequential file
					//tmp = new Center(Double.parseDouble(data[0]), Double.parseDouble(data[1]), Double.parseDouble(data[2]));
					tmp = new Element();
					for(int l = 0; l < param; l++){
						tmp.addParam(Double.parseDouble(data[l]));
					}
					//System.out.println("-------------------" + tmp.toString());
					centersFile.append(new IntWritable(i), tmp);
				}
			}
			else{
				Vector<Integer> cent = new Vector<Integer>();
				int n = 0;
				int idx = 0;
				int id = 0;

				//Create k random number
				for(int i=0; i<k; i++){
					do {
						n = (int)(Math.random()*(maxNumber-1));
					} while (cent.contains(n));
					cent.add(n);
				}


				//add centroids comparing actual index with the choosen one
				//and stop when all centroids are choosen
				while(!cent.isEmpty()){

					line = brData.readLine();
					
					if(cent.contains(idx)){
						
						String[] data = line.split(split);
						
						//read dataset's rows and traduce it into a sequential file
						tmp = new Element();
						for(int l = 0; l < param; l++){
							tmp.addParam(Double.parseDouble(data[l]));
						}
						//System.out.println("-------------------" + tmp.toString());
						centersFile.append(new IntWritable(id), tmp);
						//remove centroid because selected
						cent.removeElement(idx);
						
						id++;
					}

					idx++;
				}
			}

			//close sequence file and dataset file
			brData.close();
			centersFile.close();

		}catch (Exception e) {
			e.printStackTrace();
		}


		
	
	}



	public static void main(String[] args) throws Exception {

		if(args.length != 7){
			//check the right use of the program
			System.out.println("- USE:");
			System.out.println("$HADOOP_HOME/bin/hadoop jar KMeans.jar input_dir output_dir number_centers number_parameters loop max_line_num split_char");
		}
		else{
			Configuration conf = new Configuration();

			//centroids sequential file
			Path centers = new Path("centers/cent.seq");
			//txt file cointaining readable final centroids
			Path centersTxt = new Path("centers/cent.txt");
			//input path
			Path input = new Path(args[0]);
			//output path
			Path output = new Path(args[1]);
			//number of centroids we want use
			int numCenters = Integer.parseInt(args[2]);
			//number of parameters to use
			int numParams = Integer.parseInt(args[3]);
			//number of loop
			int loop = Integer.parseInt(args[4]);
			//max value to select random centroids
			int maxNum = Integer.parseInt(args[5]);
			//separator
			String split = args[6];
			//centroids convergence
			boolean converge = false;

			if(loop != 0){
				converge = true;
			}
			
			if(split.equals("t")){
				split = "\\t";
			}

			//set parameters in the configuration of the system for using it during the execution of Mapper, Reduce and Combiner
			conf.set("centersPath", centers.toString());
			conf.setInt("numParams", numParams);
			conf.set("split", split);


			FileSystem fs = FileSystem.get(conf);

			JobClient my_client = new JobClient();

			//centroids creation
			createCenter(numCenters, numParams, conf, input, centers, maxNum, split);
			int n = 0;

			//for(int n = 0; n < loop; n++){

			Date date;
			long start, end;

			date = new Date();
			start = date.getTime();
			//stop the loop if centroids converge or reach given number
			while((!converge) ||  (n < loop)){

				//conf.setInt("number", n);
			
				//job creation and configuration
				Job job_conf = Job.getInstance(conf, "KMeans");
				job_conf.setJarByClass(KMeansDriver.class);

				// Specify data type of output key and value
				job_conf.setOutputKeyClass(IntWritable.class);
				job_conf.setOutputValueClass(Element.class);

				// Specify names of Mapper, Combiner and Reducer Class
				job_conf.setMapperClass(KMeansMapper.class);
				job_conf.setCombinerClass(KMeansCombiner.class);
				job_conf.setReducerClass(KMeansReducer.class);
				
				//set output and input paths
				FileInputFormat.setInputPaths(job_conf, input);
				FileOutputFormat.setOutputPath(job_conf, output);
				
				//set mapper output value
				job_conf.setMapOutputKeyClass(IntWritable.class);
				job_conf.setMapOutputValueClass(Element.class);

				//run the job
				job_conf.waitForCompletion(true);

				//read counter value to check the convergence
				if(job_conf.getCounters().findCounter(KMeansReducer.CONVERGENCE.CONVERGE).getValue() == 0)
				{
					converge = true;
				}

				//delete output to execute another time
				fs.delete(output, true);
				n++;
			}

			//run only Mapper to get dataset assigned with corresponding centroids as output
			Job job_conf = Job.getInstance(conf, "KMeans");
			job_conf.setJarByClass(KMeansDriver.class);

			job_conf.setMapperClass(KMeansMapper.class);

			FileInputFormat.setInputPaths(job_conf, input);
			FileOutputFormat.setOutputPath(job_conf, output);
			
			job_conf.setMapOutputKeyClass(IntWritable.class);
			job_conf.setMapOutputValueClass(Element.class);

			job_conf.waitForCompletion(true);

			date = new Date();
			end = date.getTime();

			System.out.println(" - Execution time (milliseconds): "+(end-start));
			System.out.println(" - Execution time (seconds): "+(end-start)*0.001F);

			try {
				
				//print the output and create a readable output
				//System.out.println("------------------- L E T T U R A    F I L E S E Q    A G G I O R N A T O -------------------");

				FSDataOutputStream fsdos = fs.create(centersTxt, true);

				SequenceFile.Reader centRead = new SequenceFile.Reader(conf, SequenceFile.Reader.file(centers));
				IntWritable key = new IntWritable();
				Element cent = new Element();
				Element tmp1;
				//System.out.println("++++++++++++++++++++++");
				while(centRead.next(key, cent)){
					tmp1 = new Element(cent.getParam());
					//System.out.println("----------"+key.toString()+"---------" + tmp1.toString());
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
