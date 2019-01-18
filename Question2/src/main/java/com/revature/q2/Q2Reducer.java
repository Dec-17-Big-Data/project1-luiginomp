package com.revature.q2;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Q2Reducer extends Reducer <Text, DoubleWritable, Text, DoubleWritable> {
	
	  @Override
	  public void reduce(Text indicator, Iterable<DoubleWritable> increases, Context context) throws IOException, InterruptedException {
		  System.out.println("Q2Reducer called and passed indicator " + indicator + " and increases" + increases);
		  Double sum = 0.0;
		  Integer counter = 0;
		  for(DoubleWritable increase : increases){
			  sum += increase.get();
			  counter += 1;
		  }
		  Double average = sum / counter;
		  System.out.println("Q2Reducer writing key " + indicator + " and value " + average);
		  context.write(new Text(indicator), new DoubleWritable(average));
	  }
}