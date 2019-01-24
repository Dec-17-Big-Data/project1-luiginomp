package com.revature.q5;

import java.io.IOException;
import java.math.BigDecimal;
import java.text.DecimalFormat;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
/**
 * Mapper for MapReduce solution to Question 5.
 * Extends Mapper class from org.apache.hadoop.mapreduce.Mapper.
 * @author Luigino Perez
 *
 */
public class Q5Mapper extends Mapper <LongWritable, Text, Text, DoubleWritable>{
	String[] fields = null;
	/**
	 * Calls method emit(String category, Context context) for each year the line meets the criteria for employment under agriculture, 
	 * industry, and service. Depending on the category, will pass in Agriculture Female, Agriculture Male, Industry Female, Industry Male,
	 * Service Female, Service Male.
	 * Assumes input file given  to job is Gender_StatsData.csv from Revature project 1 requirements.
	 * Assumes that Agriculture, Industry, and Service equal 100% of working population when added together.
	 * Breaks value into String Array based on csv separators.
	 * @param key - LongWritable passed in during file input split
	 * @param value - Text represented as a line passed in during file input split
	 * @param context - Context object for entire job
	 */
	@Override
	public void map(LongWritable inKey, Text inValue, Context context) throws IOException, InterruptedException{
		//Clean data of ",["]. Note, does not get rid of initial "
		fields = inValue.toString().split("\",\"?");
		if(fields[1].equals("USA")){
			if(fields[3].equals("SL.AGR.EMPL.FE.ZS")){
				emit("Agriculture Female", context);
			}else if(fields[3].equals("SL.AGR.EMPL.MA.ZS")){
				emit("Agriculture Male", context);
			}else if(fields[3].equals("SL.IND.EMPL.FE.ZS")){
				emit("Industry Female", context);
			}else if(fields[3].equals("SL.IND.EMPL.MA.ZS")){
				emit("Industry Male", context);
			}else if(fields[3].equals("SL.SRV.EMPL.FE.ZS")){
				emit("Service Female", context);
			}else if(fields[3].equals("SL.SRV.EMPL.MA.ZS")){
				emit("Service Male", context);
			}
		}
	}
	
	/**
	 * Called by map function.
	 * Emits {Country Category Gender, change between current and last percentage} as key value pair for each new percentage.
	 * Does not emit if there are no percentages or if there is only one percentage.
	 * @param category - passed in by map function
	 * @param context - passed in map function
	 * @throws IOException
	 * @throws InterruptedException
	 */
	private void emit(String category, Context context) throws IOException, InterruptedException{
		Double entry = null;
		Double oldPercentage = null;
		Double newPercentage = null;
		for(int i = 4; i <= fields.length -1; i++){
			try{
				entry = Double.parseDouble(fields[i]);
			}catch(NumberFormatException e){
				continue;
			}catch(NullPointerException e){
				continue;
			}
			if(oldPercentage == null){
				oldPercentage = entry;
				newPercentage = entry;
				continue;
			}
			oldPercentage = newPercentage;
			newPercentage = entry;
			BigDecimal bChange = new BigDecimal((newPercentage - oldPercentage) / oldPercentage);
			String sChange = null;
			Double change = null;
			try{
				 sChange = bChange.toString().substring(0, 6);
				 change = Double.parseDouble(sChange);
			}catch(Exception e){
				DecimalFormat df = new DecimalFormat("#.##");
				change = Double.parseDouble(bChange.toString());
				change = Double.parseDouble(df.format(change));
			}
			context.write(new Text(category), new DoubleWritable(change));
		}
	}
}
