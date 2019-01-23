package com.revature.q5;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
/**
 * Reducer for MapReduce solution to Question 5.
 * Extends Reducer class from org.apache.hadoop.mapreduce.Reducerf.
 * @author Luigino Perez
 *
 */
public class Q5Reducer extends Reducer <Text, Text, Text, DoubleWritable>{
	/**
	 * @param key -
	 * @param value -
	 * @param context - Context object for entire job
	 */
	  @Override
	  public void reduce(Text country, Iterable<Text> values, Context context) throws IOException, InterruptedException {

	  }
}