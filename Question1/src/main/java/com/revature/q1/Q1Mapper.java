package com.revature.q1;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class Q1Mapper extends Mapper <LongWritable, Text, Text, Text> {
	
	//Identify the countries where % of female graduates is less than 30%.
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		//Split value into fields
		String[] fields = value.toString().split("\",\"?");
		//Check if the field containing indicator code matches the indicator code for female graduates
		if(fields[3].equals("SE.TER.CMPL.FE.ZS")){
			//Get the most recent data
			for(int i = fields.length -1; i > 0; i--){
				try{
					//Write a key value pair using country and percentage if you find a percentage that is less than 30%
					if(Double.parseDouble(fields[i]) < 30.00){
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
