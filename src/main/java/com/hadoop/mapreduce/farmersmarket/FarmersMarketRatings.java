package com.hadoop.mapreduce.farmersmarket;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.hadoop.mapreduce.example.WordCount;
import com.hadoop.mapreduce.example.WordCount.IntSumReducer;
import com.hadoop.mapreduce.example.WordCount.TokenizerMapper;

public class FarmersMarketRatings {
	
	public static class FarmersMarketRatingsMapper extends Mapper<Object, Text, Text, Text>
	{
		private Text loc  = new Text();
		private Text rating  = new Text();

		public void map(LongWritable inKey, Text inValue,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			String [] row = inValue.toString().split(",");
			if (row.length == 59)
			{
				String location = row[8]+","+row[10];
				int count = 0;
				int rated = 0;
				for(int i = 28; i< row.length; i++)
				{
					if(row[i].equals("Y"))
					{
						count++;
					}
				}
				
				
				count = (count*100)/31;
				if(count>0)
					rated = 1;
				
				loc.set(location);
				rating.set(1+"\t"+rated+"\t"+count); // num total, num rated, rating
				
				output.collect(loc, rating);
			}
			
		}

		
	}
	
	
	public static class FarmersMarketRatingsReducer extends Reducer<Object, Text, Text, Text>
	{

		public void reduce(Text inKey, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException 
		{
			
			int rating = 0;
			int numRated = 0;
			int numTotal = 0;
			
			while(values.hasNext())
			{
				String []tokens = (values.next().toString()).split("\t");
				int num = Integer.parseInt(tokens[1]);
				int val = Integer.parseInt(tokens[2]);
				
				if(val>0)
				{
					numRated = num+numRated;
					rating = (rating*numRated + val*num)/(numRated+num);
				}
				
				if(rating > 0)
				{
					output.collect(inKey, new Text(numTotal+","+numRated+","+rating));
				}
			}
			
		}
		
	}


  public static void main(String[] args) throws Exception {
	  
	  	Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "farmers market ratings");
	    job.setJarByClass(FarmersMarketRatings.class);
	    job.setMapperClass(FarmersMarketRatingsMapper.class);
	    job.setCombinerClass(FarmersMarketRatingsReducer.class);
	    job.setReducerClass(FarmersMarketRatingsReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    
	    
	    
	    
	    FileInputFormat.addInputPath(job, new Path("hdfs://127.0.0.1:9000/input/farmers_market_data.csv"));
	    FileOutputFormat.setOutputPath(job, new Path("hdfs://127.0.0.1:9000/output/result_farmers_market_ratings"));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);

  }
}