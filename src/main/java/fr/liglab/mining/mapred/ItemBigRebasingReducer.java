package fr.liglab.mining.mapred;

import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class ItemBigRebasingReducer extends
		Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
	
	private int currentId;
	private final IntWritable valueW = new IntWritable();

	@Override
	protected void setup(Context context) throws java.io.IOException, InterruptedException {
		// FIXME: when iterating like this, min item is 1. but otherwise output is not the same...
		this.currentId = context.getConfiguration().getInt(TopPIoverHadoop.KEY_REBASING_MAX_ID, 0);
	}
	
	@Override
	protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
			throws java.io.IOException, InterruptedException {
		
		Iterator<IntWritable> it = values.iterator();
		
		while (it.hasNext()) {
			valueW.set(currentId--);
			context.write(it.next(), valueW);
		}
	}
}
