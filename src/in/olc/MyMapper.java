package in.olc;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;




public class MyMapper extends Mapper<LongWritable,Text,Text,IntWritable>{ 
	//we should write and get the mapper program as key value pair
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		System.out.println("Key ="+key);
		System.out.println("Value ="+line);
		
		String words[]=line.split(" ");
		
		for(String word:words) {
			
			context.write(new Text(word),new IntWritable(1));
		}
	
	
	}
	
	
	
	
	
}
