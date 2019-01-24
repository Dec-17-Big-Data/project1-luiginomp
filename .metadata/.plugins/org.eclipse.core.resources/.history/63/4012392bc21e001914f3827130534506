package com.revature.q2;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Q2Reducer extends Reducer <Text, DoubleWritable, Text, DoubleWritable> {
	
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