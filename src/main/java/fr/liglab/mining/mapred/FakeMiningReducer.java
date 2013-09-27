package fr.liglab.mining.mapred;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import fr.liglab.mining.mapred.writables.ConcatenatedTransactionsWritable;

/**
 * Does nothing at all.
 */
public class FakeMiningReducer
		extends
		Reducer<IntWritable, ConcatenatedTransactionsWritable, NullWritable, NullWritable> {
	
	@Override
	protected void reduce(IntWritable arg0,
			Iterable<ConcatenatedTransactionsWritable> arg1,
			Context arg2)
			throws IOException, InterruptedException {
		
		
		
	}
}
