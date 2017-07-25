package mapreduce.demo.task2;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class Acadass61Task1Mapper  extends Mapper<LongWritable, Text, Text, IntWritable> {
	
	IntWritable one = new IntWritable(1);
	Text companytxt = new Text();
	Text statetxt = new Text();
	
	
	


//method to check if record is invald
private boolean recordIsBad(Text record) {
	// return true if record is bad by your standards
	String valstr = record.toString();
	// NA not found, so this is a good record, -1 is returned when text not
	// found
	if (valstr.indexOf("NA") == -1)
		return false;
	else
		return true;

}
	
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		//get rid of the invalid records , so that they dont add to the counts.
		if (recordIsBad(value)) {
			context.getCounter("Bad Record Counter", "Containing NA").increment(1);
		} else {
			// Company Name|Product Name|Size in inches|State|Pin Code|Price
			// Samsung|Optima|14|Madhya Pradesh|132401|14200
			//String[] lineArray = value.toString().split("[|]");
			String[] lineArray =  StringUtils.split(value.toString(),"|");
			String CompanyName=lineArray[0];
			String ProductName=lineArray[1];
			String Sizeinches=lineArray[2];
			String State=lineArray[3];
			String PinCode=lineArray[4];
			String Price=lineArray[5];
			System.out.println("From The Mapper=>CompanyName=>" + CompanyName);
			System.out.println("From The Mapper=>ProductName=>" + ProductName);
			System.out.println("From The Mapper=>State=>" + State);
			
			
			//calculate the total units sold in each state for the company.
			if (CompanyName.equalsIgnoreCase("Onida") 
				|| CompanyName.equalsIgnoreCase("Akai") 
				|| CompanyName.equalsIgnoreCase("Lava") 
				|| CompanyName.equalsIgnoreCase("Samsung") 
				|| CompanyName.equalsIgnoreCase("Zen") 
				)
			{
				
				companytxt.set(CompanyName);
				context.write(companytxt, one);
			}				
			
		
		}
	}
}