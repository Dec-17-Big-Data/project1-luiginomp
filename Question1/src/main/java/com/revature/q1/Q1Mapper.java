package com.revature.q1;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
/**
 * Mapper for MapReduce solution to Question 1.
 * Extends Mapper class from org.apache.hadoop.mapreduce.Mapper.
 * @author Luigino Perez
 *
 */
public class Q1Mapper extends Mapper <LongWritable, Text, Text, Text> {
	
	/**
	 * Checks if Text line indicates "Gross graduation ratio, tertiary, female (%)"
	 * Iterates backwards through array to get the latest percentage.
	 * If it finds a percentage below 30.00%, it will emit a key-value pair as country-percentage and end the mapping.
	 * 
	 * Assumes input file given  to job is Gender_StatsData.csv from Revature project 1 requirements.
	 * Breaks value into String Array based on csv separators.
	 * @param key - LongWritable passed in during file input split
	 * @param value - Text represented as a line passed in during file input split
	 * @param context - Context object for entire job
	 */
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		//Split value into fields
		String[] fields = value.toString().split("\",\"?");
		//Check if the field containing indicator code matches the indicator code for female graduates
		if(fields[3].equals("SE.TER.CMPL.FE.ZS")){
			//Get the most recent percentage. 4 is earliest index that can contain a percentage
			for(int i = fields.length -1; i > 4; i--){
				try{
					//Write a key value pair using country and percentage if you find a percentage that is less than 30%
					if(Double.parseDouble(fields[i]) < 30.00){
						System.out.println(fields[i]);
						//Country is taken from substring(1) because of the residual " at the beginning
						context.write(new Text(fields[0].substring(1)), new Text(fields[i]));
						i = 0;
					}
				}catch (Exception e){
				}
			}
			
		}
	}
}
