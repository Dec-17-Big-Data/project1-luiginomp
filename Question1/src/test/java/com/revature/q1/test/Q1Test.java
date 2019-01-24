package com.revature.q1.test;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

import com.revature.q1.Q1Mapper;
 
public class Q1Test {
	  MapDriver<LongWritable, Text, Text, Text> mapDriver;
	 
	  @Before
	  public void setUp() {
		    Q1Mapper mapper = new Q1Mapper();
		    mapDriver = MapDriver.newMapDriver(mapper);
	  }
	 
	  @Test
	  public void testMapper() {
		  mapDriver.withInput(new LongWritable(), new Text("\"Venezuela, RB\",\"VEN\",\"Gross graduation ratio, tertiary, female (%)\","
		  		+ "\"SE.TER.CMPL.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
		  		+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"9.45207\","
		  		+ "\"\",\"14.43412\",\"\",\"\",\"\",\"\",\"\",\"\",\"24.73485\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","));
				  mapDriver.withOutput(new Text("Venezuela, RB"), new Text("24.74"));
		  mapDriver.runTest();
	  }
}
