package com.revature.q4;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Q4Reducer extends Reducer <Text, Text, Text, DoubleWritable>{
	//List the % of change in female employment from the year 2000.
	  @Override
	  public void reduce(Text country, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		  //Split each value into the first/second marker and the percentage
		  Double firstPercentage = null;
		  Double secondPercentage = null;
		  for(Text value : values){
			  String[] fields = value.toString().split("\\s+");
			  if(fields[0].contains("first")){
				  try{
						firstPercentage = Double.parseDouble(fields[1]);
					}catch(NumberFormatException e){
					}catch(NullPointerException e){
					}				
			  }else if(fields[0].contains("second")){
				  try{
						secondPercentage = Double.parseDouble(fields[1]);
					}catch(NumberFormatException e){
					}catch(NullPointerException e){
					}				
			  }
		  }
		  if(firstPercentage == null || secondPercentage == null){
			  return;
		  }
		  //Emit key-value pair as the country and the calculated change percentage
		  Double changePercentage = (secondPercentage - firstPercentage) / firstPercentage;
		  context.write(new Text(country), new DoubleWritable(changePercentage));
	  }
}
