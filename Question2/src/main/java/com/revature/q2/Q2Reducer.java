package com.revature.q2;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
/**
 * Reducer for MapReduce solution to Question 2.
 * Extends Reducer class from org.apache.hadoop.mapreduce.Reducerf.
 * @author Luigino Perez
 *
 */
public class Q2Reducer extends Reducer <Text, DoubleWritable, Text, DoubleWritable> {
	
	/**
	 * Calculates the average increase for each graduation criteria
	 * Assumes Mapper is emitting key-value pairs as Text, DoubleWritable.
	 * @param key - Text representing graduation criteria
	 * @param value - Iterable of increases that have the same key
	 * @param context - Context object for entire job
	 */
	  @Override
	  public void reduce(Text key, Iterable<DoubleWritable> increases, Context context) throws IOException, InterruptedException {
		  //Calculate the average of increases by getting their sum and dividing by the count
		  Double sum = 0.0;
		  Integer counter = 0;
		  for(DoubleWritable increase : increases){
			  sum += increase.get();
			  counter += 1;
		  }
		  Double average = sum / counter;
		  context.write(new Text(key), new DoubleWritable(average));
	  }
}