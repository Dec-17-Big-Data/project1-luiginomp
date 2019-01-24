package com.revature.q5;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.DecimalFormat;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
/**
 * Reducer for MapReduce solution to Question 5.
 * Extends Reducer class from org.apache.hadoop.mapreduce.Reducer.
 * @author Luigino Perez
 *
 */
public class Q5Reducer extends Reducer <Text, DoubleWritable, Text, Text>{
	/**
	 * Calculates the average for all values for a given key
	 * @param key - String containing Country name, category, gender
	 * @param values - Iterable of percentages from pairs with same key
	 * @param context - Context object for entire job
	 */
	  @Override
	  public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
		  Double sum = 0.0;
		  Integer counter = 0;
		  for(DoubleWritable value : values){
			  sum += value.get();
			  counter += 1;
		  }
			BigDecimal bAverage = new BigDecimal((sum/counter) * 100);
			String sAverage = bAverage.toString().substring(0, 6);
			Double average = Double.parseDouble(sAverage);
		  context.write(key, new Text(average.toString()));
	  }
}