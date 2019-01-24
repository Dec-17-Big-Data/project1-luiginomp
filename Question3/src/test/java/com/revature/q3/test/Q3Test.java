package com.revature.q3.test;

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

import com.revature.q3.Q3Mapper;
import com.revature.q3.Q3Reducer;
 
public class Q3Test {
	  MapDriver<LongWritable, Text, Text, Text>  mapDriver;
	  ReduceDriver <Text, Text, Text, DoubleWritable> reduceDriver;
	  MapReduceDriver<LongWritable, Text, Text, Text, Text, DoubleWritable> mapReduceDriver;
	  
	  @Before
	  public void setUp() {
		    Q3Mapper mapper = new Q3Mapper();
		    Q3Reducer reducer = new Q3Reducer();
		    mapDriver = MapDriver.newMapDriver(mapper);
		    reduceDriver = ReduceDriver.newReduceDriver(reducer);
		    mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
	  }
	 
	  @Test
	  public void testMapper() {
		  mapDriver.withInput(new LongWritable(), new Text("\"United States\",\"USA\",\"Employment to population ratio, 15+,"
		  		+ " male (%) (modeled ILO estimate)\",\"SL.EMP.TOTL.SP.MA.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
		  		+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
		  		+ "\"69.302001953125\",\"68.802001953125\",\"69.0240020751953\",\"69.5419998168945\",\"69.9820022583008\","
		  		+ "\"70.1650009155273\",\"70.6579971313477\",\"70.9810028076172\",\"71.1549987792969\",\"71.2910003662109\","
		  		+ "\"70.1220016479492\",\"68.7910003662109\",\"67.9130020141602\",\"68.1800003051758\",\"68.5139999389648\","
		  		+ "\"69.0100021362305\",\"68.6940002441406\",\"67.463996887207\",\"63.5250015258789\",\"62.6809997558594\","
		  		+ "\"62.9599990844727\",\"63.6580009460449\",\"63.6969985961914\",\"64.2409973144531\",\"64.7129974365234\","
		  		+ "\"64.8519973754883\","));
		  mapDriver.withOutput(new Text("United States"), new Text("first 71.2910003662109"));
		  mapDriver.withOutput(new Text("United States"), new Text("second 64.8519973754883"));
		  mapDriver.runTest();
	  }
	  
	  @Test
	  public void testReducer() {
		    List<Text> values = new ArrayList<Text>();
		    values.add(new Text("first 71.2910003662109"));
		    values.add(new Text("second 64.8519973754883"));
		    reduceDriver.withInput(new Text("United States"), values);
		    reduceDriver.withOutput(new Text("United States"), new DoubleWritable(-9.031));
		    reduceDriver.runTest();
	  }
	  
	  @Test
	  public void testMapReduce() {
		    mapReduceDriver.withInput(new LongWritable(), new Text("\"United States\",\"USA\",\"Employment to population ratio, 15+,"
		    		+ " male (%) (modeled ILO estimate)\",\"SL.EMP.TOTL.SP.MA.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
		    		+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
		    		+ "\"69.302001953125\",\"68.802001953125\",\"69.0240020751953\",\"69.5419998168945\",\"69.9820022583008\","
		    		+ "\"70.1650009155273\",\"70.6579971313477\",\"70.9810028076172\",\"71.1549987792969\",\"71.2910003662109\","
		    		+ "\"70.1220016479492\",\"68.7910003662109\",\"67.9130020141602\",\"68.1800003051758\",\"68.5139999389648\","
		    		+ "\"69.0100021362305\",\"68.6940002441406\",\"67.463996887207\",\"63.5250015258789\",\"62.6809997558594\","
		    		+ "\"62.9599990844727\",\"63.6580009460449\",\"63.6969985961914\",\"64.2409973144531\",\"64.7129974365234\","
		    		+ "\"64.8519973754883\","));
		    mapReduceDriver.withOutput(new Text("United States"), new DoubleWritable(-9.031));
		    mapReduceDriver.runTest();
	  }
}
