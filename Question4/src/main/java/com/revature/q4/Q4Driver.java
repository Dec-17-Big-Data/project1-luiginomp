package com.revature.q4;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Q4Driver {

	
	public static void main(String[] args) throws Exception{
		//Check if two arguments were passed from command line
	    if (args.length != 2) {
	        System.out.printf("Usage: driver <input dir> <output dir>\n");
	        System.exit(-1);
	      }
	    Job job = new Job();
	    job.setJarByClass(Q4Driver.class);
	    job.setJobName("Question 3");
	    FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapperClass(Q4Mapper.class);
		job.setReducerClass(Q4Reducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(DoubleWritable.class);
	    //System exit as 0 if the job completes successfully, otherwise exit as 1
	    boolean success = job.waitForCompletion(true);
	    System.exit(success ? 0 : 1);
	}
	
}