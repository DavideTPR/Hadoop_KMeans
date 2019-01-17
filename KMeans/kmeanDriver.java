
package KMean;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.Configured;
//import org.apache.hadoop.fs.FileStatus;
//import org.apache.hadoop.conf.Configuration;

public class KMeanDriver {


	private static void createCenter(int k, configuration conf, Path input, Path centers){
		SequenceFile.Writer centers = new SequenceFile.CreateWriter(conf, SequenceFile.Writer.file(centers), SequenceFile.Writer.keyClass(IntWritable.class), SequenceFile.Writer.valueClass(Center.class));
	
		try {

			FileSystem fs = FileSystem.get(new Configuration());
			FileStatus[] ri = fs.listStatus(input);

			/*for (int i = 0; i < ri.length; i++) {
				System.out.println(i + "-------------------------------------" + ri[i].getPath());
			}*/

			BufferReader brData = new BufferReader(new FileReader(ri[0].getPath()));
			String line;
			Center tmp;

			for(int i=0; i<k; i++)
			{
				line = brData.readLine();
				String[] data = line.split('\t');

				tmp = new Center(Double.parseDouble(SingleData[0]), Double.parseDouble(SingleData[1]), Double.parseDouble(SingleData[2]));

				centers.append(new IntWritable(i), tmp);
			}


			brData.close();
		}catch (Exception e) {
			e.printStackTrace();
		}


		centers.close();
	
	}



	public static void main(String[] args) {

		//TODO
		Configuration conf = new Configuration();

		Path centers = new Path("centers/cent.seq");
		Path input = new Path(args[0]);

		JobClient my_client = new JobClient();
		// Create a configuration object for the job
		JobConf job_conf = new JobConf(KMeanDriver.class);

		// Set a name of the Job
		job_conf.setJobName("KMeans");

		// Specify data type of output key and value
		job_conf.setOutputKeyClass(Integer.class);
		job_conf.setOutputValueClass(Center.class);

		// Specify names of Mapper and Reducer Class
		job_conf.setMapperClass(KMean.KMeanMapper.class);
		job_conf.setCombinerClass(KMean.KMeanCombiner.class);
		job_conf.setReducerClass(KMean.KMeanReducer.class);

		// Specify formats of the data type of Input and output
		job_conf.setInputFormat(TextInputFormat.class);
		job_conf.setOutputFormat(TextOutputFormat.class);

		// Set input and output directories using command line arguments, 
		//arg[0] = name of input directory on HDFS, and arg[1] =  name of output directory to be created to store the output file.
		
		FileInputFormat.setInputPaths(job_conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(job_conf, new Path(args[1]));
		


		my_client.setConf(job_conf);
		try {
			// Run the job 
			JobClient.runJob(job_conf);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
