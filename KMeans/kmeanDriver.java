
package KMean;


import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.FileReader;


import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
//import org.apache.hadoop.fs.FileStatus;
//import org.apache.hadoop.conf.Configuration;

public class KMeanDriver {


	private static void createCenter(int k, Configuration conf, Path input, Path centers){
	
		try {
			SequenceFile.Writer centersFile = new SequenceFile.createWriter(conf, SequenceFile.Writer.file(centers), SequenceFile.Writer.keyClass(IntWritable.class), SequenceFile.Writer.valueClass(Center.class));


			FileSystem fs = FileSystem.get(new Configuration());
			FileStatus[] ri = fs.listStatus(input);

			//SequenceFile.Writer centersFile = new SequenceFile.Writer(fs, conf, centers, IntWritable.class, Center.class);

			/*for (int i = 0; i < ri.length; i++) {
				System.out.println(i + "-------------------------------------" + ri[i].getPath());
			}*/

			BufferedReader brData = new BufferedReader(new InputStreamReader(fs.open(ri[0].getPath())));
			String line;
			Center tmp;

			for(int i=0; i<k; i++)
			{
				line = brData.readLine();
				String[] data = line.split("\\t");

				tmp = new Center(Double.parseDouble(data[0]), Double.parseDouble(data[1]), Double.parseDouble(data[2]));
				System.out.println("-------------------" + tmp.toString());
				centersFile.append(new IntWritable(i), tmp);
			}

			brData.close();
			centersFile.close();

		}catch (Exception e) {
			e.printStackTrace();
		}


		
	
	}



	public static void main(String[] args) {

		//TODO
		Configuration conf = new Configuration();
		//System.out.println("111111111111111111111111111111111111111111111111111111111111111111111");

		Path centers = new Path("centers/cent.seq");
		Path input = new Path(args[0]);
		//System.out.println("2222222222222222222222222222222222222222222222222222222222222222222222");
		conf.set("centersPath", centers.toString());

		JobClient my_client = new JobClient();
		// Create a configuration object for the job
		JobConf job_conf = new JobConf(KMeanDriver.class);
		//System.out.println("3333333333333333333333333333333333333333333333333333333333333333333333");
		createCenter(5, conf, input, centers);

		// Set a name of the Job
		job_conf.setJobName("KMeans");
		//System.out.println("44444444444444444444444444444444444444444444444444444444444444444444444");
		// Specify data type of output key and value
		job_conf.setOutputKeyClass(IntWritable.class);
		job_conf.setOutputValueClass(Center.class);
		//System.out.println("55555555555555555555555555555555555555555555555555555555555555555555555");
		// Specify names of Mapper and Reducer Class
		job_conf.setMapperClass(KMean.KMeanMapper.class);
		job_conf.setCombinerClass(KMean.KMeanCombiner.class);
		job_conf.setReducerClass(KMean.KMeanReducer.class);
		//System.out.println("66666666666666666666666666666666666666666666666666666666666666666666666");
		// Specify formats of the data type of Input and output
		job_conf.setInputFormat(TextInputFormat.class);
		job_conf.setOutputFormat(TextOutputFormat.class);
		//System.out.println("777777777777777777777777777777777777777777777777777777777777777777777777");
		// Set input and output directories using command line arguments, 
		//arg[0] = name of input directory on HDFS, and arg[1] =  name of output directory to be created to store the output file.
		//System.out.println("88888888888888888888888888888888888888888888888888888888888888888888888888");
		FileInputFormat.setInputPaths(job_conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(job_conf, new Path(args[1]));
		//System.out.println("999999999999999999999999999999999999999999999999999999999999999999999999999");


		my_client.setConf(job_conf);
		//System.out.println("0000000000000000000000000000000000000000000000000000000000000000000000000000");
		try {
			// Run the job 
			JobClient.runJob(job_conf);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
