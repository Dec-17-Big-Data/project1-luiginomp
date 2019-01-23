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
	
	  MapDriver<LongWritable, Text, Text, Text>  mapDriver;
	  ReduceDriver <Text, Text, Text, DoubleWritable> reduceDriver;
	  MapReduceDriver<LongWritable, Text, Text, Text, Text, DoubleWritable> mapReduceDriver;
	  
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
//		  mapDriver.withInput(new LongWritable(), new Text("\"United States\",\"USA\",\"Employment to population ratio, 15+, female (%) (modeled ILO estimate)\",\"SL.EMP.TOTL.SP.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"52.5610008239746\",\"52.5690002441406\",\"52.9469985961914\",\"54.1669998168945\",\"54.6139984130859\",\"55.0340003967285\",\"55.7900009155273\",\"56.1259994506836\",\"56.4620018005371\",\"56.6199989318848\",\"56.1189994812012\",\"55.2980003356934\",\"55.1599998474121\",\"55.0439987182617\",\"55.3009986877441\",\"55.7270011901855\",\"55.7519989013672\",\"55.3969993591309\",\"53.5410003662109\",\"52.6469993591309\",\"52.1679992675781\",\"52.2569999694824\",\"52.3470001220703\",\"52.693000793457\",\"53.1230010986328\",\"53.2099990844727\","));
//		  mapDriver.withOutput(new Text("United States"), new Text("first 56.6199989318848"));
//		  mapDriver.withOutput(new Text("United States"), new Text("second 53.2099990844727"));
//		  mapDriver.runTest();
	  }
	  
	  @Test
	  public void testReducer() {
//		    List<Text> values = new ArrayList<Text>();
//		    values.add(new Text("first 20"));
//		    values.add(new Text("second 10"));
//		    reduceDriver.withInput(new Text("United States"), values);
//		    reduceDriver.withOutput(new Text("United States"), new DoubleWritable(-0.5));
//		    reduceDriver.runTest();
	  }
	  
	  @Test
	  public void testMapReduce() {
//		    mapReduceDriver.withInput(new LongWritable(), new Text("\"United States\",\"USA\",\"Employment to population ratio, 15+, female (%) (modeled ILO estimate)\",\"SL.EMP.TOTL.SP.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"52.5610008239746\",\"52.5690002441406\",\"52.9469985961914\",\"54.1669998168945\",\"54.6139984130859\",\"55.0340003967285\",\"55.7900009155273\",\"56.1259994506836\",\"56.4620018005371\",\"56.6199989318848\",\"56.1189994812012\",\"55.2980003356934\",\"55.1599998474121\",\"55.0439987182617\",\"55.3009986877441\",\"55.7270011901855\",\"55.7519989013672\",\"55.3969993591309\",\"53.5410003662109\",\"52.6469993591309\",\"52.1679992675781\",\"52.2569999694824\",\"52.3470001220703\",\"52.693000793457\",\"53.1230010986328\",\"53.2099990844727\","));
//		    mapReduceDriver.withOutput(new Text("United States"), new DoubleWritable(-0.060226066968217584));
//		    mapReduceDriver.runTest();
	  }

}