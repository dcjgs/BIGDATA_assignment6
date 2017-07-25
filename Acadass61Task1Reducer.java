package mapreduce.demo.task2;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class Acadass61Task1Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	  
	  @Override
	  public void reduce(Text key, Iterable<IntWritable> values,
	      Context context)
	      throws IOException, InterruptedException {
	      System.out.println("Acadass61Task1Reducer:From The Reducer=>"+key) ;	
	    
	      int sum = 0;
	      for (IntWritable value : values) {
			sum+=value.get();	
	       }		
	      //company, count
	       context.write(key, new IntWritable(sum));
	  }
}