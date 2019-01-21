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
		  mapDriver.withInput(new LongWritable(), new Text("\"Albania\",\"ALB\",\"Gross graduation ratio, tertiary, female (%)\",\"SE.TER.CMPL.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"12.39418\",\"\",\"\",\"15.0017\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"39.70473\",\"48.23856\",\"51.73209\",\"43.19789\",\"47.20538\",\"\","));
		  mapDriver.withOutput(new Text("Albania"), new Text("15.0017"));
		  mapDriver.runTest();
	  }
}
