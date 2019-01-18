package com.revature.q2.test;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import com.revature.q2.Q2Mapper;
 
public class Q2Test {
	  MapDriver<LongWritable, Text, Text, DoubleWritable>  mapDriver;
	  ReduceDriver <Text, DoubleWritable, Text, DoubleWritable> reduceDriver;
	  
	  @Before
	  public void setUp() {
		    Q2Mapper mapper = new Q2Mapper();
		    mapDriver = MapDriver.newMapDriver(mapper);
	  }
	 
	  @Test
	  public void testMapper() {
		  //TODO Implement
	  }
	  
	  @Test
	  public void testReducer() {
		  //TODO Implement
	  }
	  
	  @Test
	  public void testMapReduce() {
		  //TODO Implement
	  }
}
