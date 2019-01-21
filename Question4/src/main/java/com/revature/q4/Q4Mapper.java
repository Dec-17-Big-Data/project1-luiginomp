package com.revature.q4;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Q4Mapper extends Mapper <LongWritable, Text, Text, Text>{
	//List the % of change in female employment from the year 2000.
	/*
	 * ASSUMPTIONS
	 * Need results from every country with available data
	 * Don't include any countries that don't have any available data
	 * 
	 * APPROACH
	 * Employment to population ratio, 15+, female (%) (modeled ILO estimate)	SL.EMP.TOTL.SP.FE.ZS
	 * Present results by country and its percentage
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
