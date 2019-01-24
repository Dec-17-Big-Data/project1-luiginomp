package com.revature.q2;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.DecimalFormat;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
/**
 * Mapper for MapReduce solution to Question 2.
 * Extends Mapper class from org.apache.hadoop.mapreduce.Mapper.
 * @author Luigino Perez
 *
 */
public class Q2Mapper extends Mapper <LongWritable, Text, Text, DoubleWritable> {

	/**
	 * Checks if given information is for USA and if criteria involes female graduations for at least primary, at least secondary, or at least post-secondary.
	 * Calculates increase between each concurrent percentage and emits a key-value pair as graduation type and calculated increase.
	 * 
	 * Assumes input file given  to job is Gender_StatsData.csv from Revature project 1 requirements.
	 * Breaks value into String Array based on csv separators.
	 * @param key - LongWritable passed in during file input split
	 * @param value - Text represented as a line passed in during file input split
	 * @param context - Context object for entire job
	 */
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		//Cleanse data
		String[] fields = value.toString().split("\",\"?");
		//Check if country code field contains "USA"
		if(fields[1].equals("USA")){
			//Check if indicator code field meets the criteria for primary, secondary, or post-secondary education
			if(fields[3].equals("SE.PRM.CUAT.FE.ZS") || 
					fields[3].equals("SE.SEC.CUAT.UP.FE.ZS") || 
					fields[3].equals("SE.SEC.CUAT.PO.FE.ZS")){
				DecimalFormat df = new DecimalFormat("#.####");
				df.setRoundingMode(RoundingMode.CEILING);
				Double currentPercentage = null;
				Double lastPercentage = null;
				//Start from year 2000 and calculate the increase between every new entry and previous entry
				for(int i = 44; i <= fields.length -1; i++){
					Double entry = null;
					try{
						entry = Double.parseDouble(fields[i]);
					}catch(NumberFormatException e){
						continue;
					}catch(NullPointerException e){
						continue;
					}
					if(lastPercentage == null){
						lastPercentage = entry;
						currentPercentage = entry;
						continue;
					}
					lastPercentage = currentPercentage;
					currentPercentage = entry;
					BigDecimal bIncrease = new BigDecimal(((currentPercentage - lastPercentage) / lastPercentage) * 100);
					String sIncrease = bIncrease.toString().substring(0, 6);
					Double increase = Double.parseDouble(sIncrease);
					String category = null;
					//Change which key is written dependent on the indicator code field
					if(fields[3].equals("SE.PRM.CUAT.FE.ZS")){
						category = "Primary";
					}else if(fields[3].equals("SE.SEC.CUAT.UP.FE.ZS")){
						category = "Secondary";
					}else if(fields[3].equals("SE.SEC.CUAT.PO.FE.ZS")){
						category = "Post-Secondary";
					}
					context.write(new Text(category), new DoubleWritable(Double.parseDouble(increase.toString())));
				}
			}
		}
	}
}