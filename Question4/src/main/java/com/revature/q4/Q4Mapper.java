package com.revature.q4;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
/**
 * Mapper for MapReduce solution to Question 4.
 * Extends Mapper class from org.apache.hadoop.mapreduce.Mapper.
 * @author Luigino Perez
 *
 */
public class Q4Mapper extends Mapper <LongWritable, Text, Text, Text>{
	/**
	 * Checks if input contains information for female employment %.
	 * Emits two key-value pairs: 
	 * {Country name, "first " + earliest percentage since 2000} and
	 * {Country name, "second " + latest percentage}
	 * 
	 * Assumes input file given  to job is Gender_StatsData.csv from Revature project 1 requirements.
	 * Breaks value into String Array based on csv separators.
	 * @param key - LongWritable passed in during file input split
	 * @param value - Text represented as a line passed in during file input split
	 * @param context - Context object for entire job
	 */
	@Override
	public void map(LongWritable inKey, Text inValue, Context context) throws IOException, InterruptedException{
		//Clean data of ",["]. Note, does not get rid of initial "
		String[] fields = inValue.toString().split("\",\"?");
		//Check if line matches category
		if(fields[3].contains("SL.EMP.TOTL.SP.FE.ZS")){
			//Get earliest entry, since 2000 (2000 is index 44) as well as the year
			Double earliestEntry = null;
			for(int i = 44; i < fields.length - 1; i++){
				try{
					earliestEntry = Double.parseDouble(fields[i]);
					break;
				}catch(NumberFormatException e){
					continue;
				}catch(NullPointerException e){
					continue;
				}
			}
			if(earliestEntry == null){
				return;
			}
			//Get key as the country name. Remember to remove leftover "
			String country = fields[0].toString().substring(1);
			//Get value as string to contain notation "first" or "second" to let reducer now how to calculate the percentage
			String value = "first " + earliestEntry.toString();
			//Emit first key-value pair
			context.write(new Text(country), new Text(value));
			//Get latest entry
			Double latestEntry = null;
			for(int i = fields.length - 1; i > 44; i--){
				try{
					latestEntry = Double.parseDouble(fields[i]);
					break;
				}catch(NumberFormatException e){
					continue;
				}catch(NullPointerException e){
					continue;
				}
			}
			//Emit second key-value pair
			value = "second " + latestEntry.toString();
			context.write(new Text(country), new Text(value));
		}
	}
}
