package com.revature.q1;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
/**
 * Driver for MapReduce solution to Question 1. Used to configure job.
 * @author Luigino Perez
 */
public class Q1Driver {

	/**
	 * Assumes input file is Gender_StatsData.csv.
	 * Configures settings for MapReduce Job.
	 * Job name: "Question 1".
	 * Sets number of reduces to 0 because only Mapper is used for this job.
	 * Output key and output value are Text.
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
	    job.setJarByClass(Q1Driver.class);
	    job.setJobName("Question 1");
	    FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapperClass(Q1Mapper.class);
		//Set number of reducers to 0 because just does not use reducers
		job.setNumReduceTasks(0);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    //System exit as 0 if the job completes successfully, otherwise exit as 1
	    boolean success = job.waitForCompletion(true);
	    System.exit(success ? 0 : 1);
	}
	
}
