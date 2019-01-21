package com.revature.q2.test;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import com.revature.q2.Q2Mapper;
import com.revature.q2.Q2Reducer;
 
public class Q2Test {
	  MapDriver<LongWritable, Text, Text, DoubleWritable>  mapDriver;
	  ReduceDriver <Text, DoubleWritable, Text, DoubleWritable> reduceDriver;
	  MapReduceDriver<LongWritable, Text, Text, DoubleWritable, Text, DoubleWritable> mapReduceDriver;
	  
	  @Before
	  public void setUp() {
		    Q2Mapper mapper = new Q2Mapper();
		    Q2Reducer reducer = new Q2Reducer();
		    mapDriver = MapDriver.newMapDriver(mapper);
		    reduceDriver = ReduceDriver.newReduceDriver(reducer);
		    mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
	  }
	 
	  @Test
	  public void testMapper() {
		  mapDriver.withInput(new LongWritable(), new Text("\"United States\",\"USA\",\"Educational attainment, at least completed primary, population 25+ years, female (%) (cumulative)\",\"SE.PRM.CUAT.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"20.00\",\"75.00\",\"\",\"\",\"\","));
		  mapDriver.withOutput(new Text("Primary"), new DoubleWritable(2.75));
		  mapDriver.runTest();
	  }
	  
	  @Test
	  public void testReducer() {
		    List<DoubleWritable> values = new ArrayList<DoubleWritable>();
		    values.add(new DoubleWritable(23.64));
		    values.add(new DoubleWritable(19.52));
		    reduceDriver.withInput(new Text("Primary"), values);
		    reduceDriver.withOutput(new Text("Primary"), new DoubleWritable(21.58));
		    reduceDriver.runTest();
	  }
	  
	  @Test
	  public void testMapReduce() {
		    mapReduceDriver.withInput(new LongWritable(), new Text("\"United States\",\"USA\",\"Educational attainment, at least completed primary, population 25+ years, female (%) (cumulative)\",\"SE.PRM.CUAT.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"10.00\",\"40.00\",\"\",\"\",\"20.00\",\"75.00\",\"\",\"\",\"\","));
		    mapReduceDriver.withOutput(new Text("Primary"), new DoubleWritable(1.75));
		    mapReduceDriver.runTest();
	  }
}
