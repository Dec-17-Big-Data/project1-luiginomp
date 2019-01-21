package com.revature.q2;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class Q2Mapper extends Mapper <LongWritable, Text, Text, DoubleWritable> {
	
	//List the average increase in female education in the U.S. from the year 2000.
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		//Cleanse data
		String[] fields = value.toString().split("\",\"");
		//Check if country code field contains "USA"
		if(fields[1].equals("USA")){
			//Check if indicator code field meets the criteria for primary, secondary, or post-secondary education
			if(fields[3].equals("SE.PRM.CUAT.FE.ZS") || 
					fields[3].equals("SE.SEC.CUAT.UP.FE.ZS") || 
					fields[3].equals("SE.SEC.CUAT.PO.FE.ZS")){
				Double currentPercentage = null;
				Double lastPercentage = null;
				//Start from year 2000 and calculate the increase between every new entry and previous entry
				for(int i = 44; i < fields.length -1; i++){
					Double entry = null;
					try{
						entry = Double.parseDouble(fields[i]);
						System.out.println("Found new entry: " + entry);
					}catch(NumberFormatException e){
						System.out.println("Skipped unparsable field: index " + i + ", " + fields[i]);
						continue;
					}catch(NullPointerException e){
						System.out.println("Skipped empty field: index " + i + ", " + fields[i]);
						continue;
					}
					if(lastPercentage == null){
						System.out.println("Need another entry to calculate");
						lastPercentage = entry;
						currentPercentage = entry;
						continue;
					}
					lastPercentage = currentPercentage;
					currentPercentage = entry;
					Double increase = (currentPercentage - lastPercentage) / lastPercentage;
					System.out.println("(" + currentPercentage + " - " + lastPercentage + ") / " + lastPercentage + " = " + increase);
					String category = null;
					//Change which key is written dependent on the indicator code field
					if(fields[3].equals("SE.PRM.CUAT.FE.ZS")){
						category = "Primary";
					}else if(fields[3].equals("SE.SEC.CUAT.UP.FE.ZS")){
						category = "Secondary";
					}else if(fields[3].equals("SE.SEC.CUAT.PO.FE.ZS")){
						category = "Post-Secondary";
					}
					context.write(new Text(category), new DoubleWritable(increase));
				}
			}
		}
	}
}