package fr.liglab.mining.mapred;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class ItemBigCountingReducer extends
		Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
	
	public static final String COUNTERS_GROUP = "ItemCounters";
	public static final String COUNTER_REBASING_MAX_ID = "rebasing-maxId";
	
	private int minSupport = 10;
	
	private final IntWritable valueW = new IntWritable();
	
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		
		this.minSupport = context.getConfiguration().getInt(TopPIoverHadoop.KEY_MINSUP, 10);
	}
	
	@Override
	protected void reduce(IntWritable key, 
			Iterable<IntWritable> values, Context context)
			throws java.io.IOException, InterruptedException {
		
		Iterator<IntWritable> it = values.iterator();
		int count = 0;
		
		while (it.hasNext()) {
			count += it.next().get();
		}
		
		if (count >= this.minSupport) {
			valueW.set(count);
			
			context.write(key, valueW);
		}
	}
}
