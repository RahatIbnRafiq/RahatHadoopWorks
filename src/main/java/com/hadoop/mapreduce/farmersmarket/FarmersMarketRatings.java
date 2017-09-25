package com.hadoop.mapreduce.farmersmarket;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FarmersMarketRatings {

  public static class MapperFarmersMarket extends Mapper<Object, Text, Text, LongWritable>{
	  private Text loc  = new Text();
	  private LongWritable rating  = new LongWritable();

	  public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
	  {
		  	String [] row = value.toString().split(",");
			if (row.length == 59)
			{
				String location = row[8]+","+row[10];
				int count = 0;
				for(int i = 28; i< row.length; i++)
				{
					if(row[i].equals("Y"))
					{
						count++;
					}
				}
				
				loc.set(location);
				rating.set(count); 
				context.write(loc, rating);
			}
    }
  }

  public static class ReducerFarmersMarket extends Reducer<Text,Text,Text,LongWritable> 
  {
	  private LongWritable result = new LongWritable();

	  public void reduce(Text inkey, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException 
	  {
		  long total = 0;
		  long totalRating = 0;
		  
		  for(LongWritable value:values)
		  {
			  total += 1;
			  totalRating+=value.get();
		  }
		  
		  result.set(totalRating/total);
		  
		  context.write(inkey, result);
		  
	  }
  	}

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "farmers market ratings");
    job.setJarByClass(FarmersMarketRatings.class);
    job.setMapperClass(MapperFarmersMarket.class);
    job.setCombinerClass(ReducerFarmersMarket.class);
    job.setReducerClass(ReducerFarmersMarket.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(LongWritable.class);
    FileInputFormat.addInputPath(job, new Path("hdfs://127.0.0.1:9000/input/farmers_market_data.csv"));
    FileOutputFormat.setOutputPath(job, new Path("hdfs://127.0.0.1:9000/output/result_farmers_market_data"));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}