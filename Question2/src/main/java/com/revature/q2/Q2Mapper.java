package com.revature.q2;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class Q2Mapper extends Mapper <LongWritable, Text, Text, DoubleWritable> {
	String[] line = null;
	
	//List the average increase in female education in the U.S. from the year 2000.
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		line = value.toString().split("\",\"");
		String countryCode = line[1];
		String indicatorCode = line[3];
		if(countryCode.equals("USA")){
			if(indicatorCode.equals("SE.PRM.CUAT.FE.ZS")){
				System.out.println("Q2Mapper found line " + indicatorCode);
				Double currentPercentage = null;
				Double lastPercentage = null;
				for(int i = 44; i < line.length -1; i++){
					Double entry = null;
					try{
						entry = Double.parseDouble(line[i]);
						System.out.println("Q2Mapper found entry " + entry);
					}catch(NumberFormatException e){
						System.out.println("Q2Mapper skipped unparsable string: '" + line[i] + "'");
						continue;
					}catch(NullPointerException e){
						System.out.println("Q2Mapper skipped empty location");
						continue;
					}
					if(lastPercentage == null){
						lastPercentage = entry;
						currentPercentage = entry;
						System.out.println("Q2Mapper needs another percentage - moving to next entry");
						continue;
					}
					lastPercentage = currentPercentage;
					currentPercentage = entry;
					Double increase = (currentPercentage - lastPercentage) / lastPercentage;
					System.out.println("Q2Mapper calculated increase from " + lastPercentage + " to " + currentPercentage + " is " + increase );
					System.out.println("Q2Mapper writing key " + indicatorCode + " and value " + increase);
					context.write(new Text(indicatorCode), new DoubleWritable(increase));
				}
			}
		}
	}
}