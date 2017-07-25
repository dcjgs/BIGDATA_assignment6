package mapreduce.demo.task2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;



public class Acadass61Task1Partitioner extends Partitioner<Text,IntWritable> {

	@Override
    public int getPartition(Text key, IntWritable value, int numReduceTasks) {
		
		String name = key.toString();
		// send to first reducer if company name is in A-F
		if (name.matches("^[A-F].*$")){	
			return 1;
			// send to second reducer if company name is in G-L
		} else if (name.matches("^[G-L].*$")) {
			return 2;
			// send to third reducer if company name is in M-R
		} else if (name.matches("^[M-R].*$")) {
			return 3;
			//default case
		} else {
			return 0;
		}		
    }
}

