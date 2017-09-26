package com.hadoop.mapreduce.earthquake;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Earthquake {
	
	public static class MapperEarthquake extends Mapper<Object, Text, Text, FloatWritable>
	{
		Text location = new Text();
		FloatWritable magnitude = new FloatWritable();
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			String row = value.toString();
			if(!row.startsWith("Src"))
			{
				String [] tokens = row.split(",");
				String loc = tokens[tokens.length -1];
				loc = loc.substring(1,loc.length()-1);
				String magString = tokens[tokens.length -4];
				magString = magString.trim();
				
				
				location.set(new Text(loc));
				magnitude.set(Float.parseFloat(magString));
				
				
				context.write(location, magnitude);
			}
		}
	}
	
	
	public static class ReducerEarthquake extends Reducer<Text, FloatWritable, Text, FloatWritable>
	{
		FloatWritable highestMagnitude = new FloatWritable();
		public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException
		{
			float highest = 0;
			for(FloatWritable value:values)
			{
				if(value.get() > highest)
					highest = value.get();
			}
			
			
			highestMagnitude.set(highest);
			context.write(key, highestMagnitude);
		}
	}

	public static void main(String[] args) throws Exception 
	{
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "earthquake");
		job.setJarByClass(Earthquake.class);
		job.setMapperClass(MapperEarthquake.class);
		job.setCombinerClass(ReducerEarthquake.class);
		job.setReducerClass(ReducerEarthquake.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class);
		
		FileInputFormat.addInputPath(job, new Path("hdfs://127.0.0.1:9000/input/earthquake.csv"));
	    FileOutputFormat.setOutputPath(job, new Path("hdfs://127.0.0.1:9000/output/result_earthquake"));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
	

}
