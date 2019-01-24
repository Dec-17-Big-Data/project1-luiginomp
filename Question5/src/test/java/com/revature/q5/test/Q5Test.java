package com.revature.q5.test;

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

import com.revature.q5.Q5Mapper;
import com.revature.q5.Q5Reducer;

public class Q5Test {
	
	  MapDriver<LongWritable, Text, Text, DoubleWritable>  mapDriver;
	  ReduceDriver <Text, DoubleWritable, Text, Text> reduceDriver;
	  MapReduceDriver<LongWritable, Text, Text, DoubleWritable, Text, Text> mapReduceDriver;
	  
	  @Before
	  public void setUp() {
		    Q5Mapper mapper = new Q5Mapper();
		    Q5Reducer reducer = new Q5Reducer();
		    mapDriver = MapDriver.newMapDriver(mapper);
		    reduceDriver = ReduceDriver.newReduceDriver(reducer);
		    mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
	  }
	 
	  @Test
	  public void testMapper() {
		  mapDriver.withInput(new LongWritable(), new Text("\"United States\",\"USA\",\"Employment in agriculture,"
		  		+ " male (% of male employment)\",\"SL.AGR.EMPL.MA.ZS\",\"10.1899995803833\",\"9.85000038146973\","
		  		+ "\"2.10999989509583\",\"2.17000007629395\",\"2.30999994277954\",\"2.27999997138977\","));
		  mapDriver.withOutput(new Text("Agriculture Male"), new DoubleWritable(-0.033));
		  mapDriver.withOutput(new Text("Agriculture Male"), new DoubleWritable(-0.785));
		  mapDriver.withOutput(new Text("Agriculture Male"), new DoubleWritable(0.0284));
		  mapDriver.withOutput(new Text("Agriculture Male"), new DoubleWritable(0.0645));
		  mapDriver.withOutput(new Text("Agriculture Male"), new DoubleWritable(-0.012));
		  mapDriver.runTest();
	  }
	  
	  @Test
	  public void testReducer() {
		  List<DoubleWritable> values = new ArrayList<DoubleWritable>();
		  values.add(new DoubleWritable(-0.033));
		  values.add(new DoubleWritable(-0.785));
		  values.add(new DoubleWritable(0.0284));
		  values.add(new DoubleWritable(0.0645));
		  values.add(new DoubleWritable(-0.012));
		  reduceDriver.withInput(new Text("Agriculture Male"), values);
		  reduceDriver.withOutput(new Text("Agriculture Male"), new Text("-14.74"));
		  reduceDriver.runTest();
	  }
	  
	  @Test
	  public void testMapReduce() {
		  mapReduceDriver.withInput(new LongWritable(), new Text("\"United States\",\"USA\",\"Employment in agriculture,"
			  		+ " male (% of male employment)\",\"SL.AGR.EMPL.MA.ZS\",\"10.1899995803833\",\"9.85000038146973\","
			  		+ "\"2.10999989509583\",\"2.17000007629395\",\"2.30999994277954\",\"2.27999997138977\","));
		  mapReduceDriver.withOutput(new Text("Agriculture Male"), new Text("-14.74"));
		  mapReduceDriver.runTest();
	  }

}
