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
		  mapDriver.withInput(new LongWritable(), new Text("\"United States\",\"USA\",\"Educational attainment,"
		    		+ " at least completed primary, population 25+ years, female (%) (cumulative)\",\"SE.PRM.CUAT.FE.ZS\","
		    		+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"95\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
		    		+ "\"96.64463\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
		    		+ "\"\",\"\",\"\",\"\",\"\",\"98.59983\",\"98.4959\",\"98.64088\",\"\",\"98.73201\",\"98.62149\",\"98.70539\","
		    		+ "\"98.70377\",\"98.82814\",\"98.86884\",\"98.76638\",\"98.76783\",\"\","));
		  mapDriver.withOutput(new Text("Primary"), new DoubleWritable(-0.105));
		  mapDriver.withOutput(new Text("Primary"), new DoubleWritable(0.1471));
		  mapDriver.withOutput(new Text("Primary"), new DoubleWritable(0.0923));
		  mapDriver.withOutput(new Text("Primary"), new DoubleWritable(-0.111));
		  mapDriver.withOutput(new Text("Primary"), new DoubleWritable(0.085));
		  mapDriver.withOutput(new Text("Primary"), new DoubleWritable(-0.001));
		  mapDriver.withOutput(new Text("Primary"), new DoubleWritable(0.126));
		  mapDriver.withOutput(new Text("Primary"), new DoubleWritable(0.0411));
		  mapDriver.withOutput(new Text("Primary"), new DoubleWritable(-0.103));
		  mapDriver.withOutput(new Text("Primary"), new DoubleWritable(0.0014));
		  mapDriver.runTest();
	  }
	  
	  @Test
	  public void testReducer() {
		    List<DoubleWritable> values = new ArrayList<DoubleWritable>();
			  values.add(new DoubleWritable(-0.105));
			  values.add(new DoubleWritable(0.1471));
			  values.add(new DoubleWritable(0.0923));
			  values.add(new DoubleWritable(-0.111));
			  values.add(new DoubleWritable(0.085));
			  values.add(new DoubleWritable(-0.001));
			  values.add(new DoubleWritable(0.126));
			  values.add(new DoubleWritable(0.0411));
			  values.add(new DoubleWritable(-0.103));
			  values.add(new DoubleWritable(0.0014));
		    reduceDriver.withInput(new Text("Primary"), values);
		    reduceDriver.withOutput(new Text("Primary"), new DoubleWritable(0.0173));
		    reduceDriver.runTest();
	  }
	  
	  @Test
	  public void testMapReduce() {
		    mapReduceDriver.withInput(new LongWritable(), new Text("\"United States\",\"USA\",\"Educational attainment,"
		    		+ " at least completed primary, population 25+ years, female (%) (cumulative)\",\"SE.PRM.CUAT.FE.ZS\","
		    		+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"95\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
		    		+ "\"96.64463\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
		    		+ "\"\",\"\",\"\",\"\",\"\",\"98.59983\",\"98.4959\",\"98.64088\",\"\",\"98.73201\",\"98.62149\",\"98.70539\","
		    		+ "\"98.70377\",\"98.82814\",\"98.86884\",\"98.76638\",\"98.76783\",\"\","));
		    mapReduceDriver.withOutput(new Text("Primary"), new DoubleWritable(0.0173));
		    mapReduceDriver.runTest();
	  }
}
