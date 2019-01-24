package com.revature.q2;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
/**
 * Driver for MapReduce solution to Question 2.
 * Used to configure job.
 * @author Luigino Perez
 */
public class Q2Driver {

	/**
	 * Configures settings for MapReduce Job.
	 * Job name: "Question 2".
	 * Sets job output key as Text, output value as Text
	 * System exits -1 if input format is incorrect. 0 if the job is successfully is completed, 1 if job is unsuccessful.
	 * @param args - String taken from command line input when running a Hadoop job. 
	 * Should contain input and output directory for HDFS.
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception{
		//Check if two arguments were passed from command line
	    if (args.length != 2) {
	        System.out.printf("Usage: driver <input dir> <output dir>\n");
	        System.exit(-1);
	      }
	    Job job = new Job();
	    job.setJarByClass(Q2Driver.class);
	    job.setJobName("Question 2");
	    FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapperClass(Q2Mapper.class);
		job.setReducerClass(Q2Reducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(DoubleWritable.class);
	    //System exit as 0 if the job completes successfully, otherwise exit as 1
	    boolean success = job.waitForCompletion(true);
	    System.exit(success ? 0 : 1);
	}
	
}
