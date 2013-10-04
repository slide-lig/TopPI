package fr.liglab.mining.mapred;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

import fr.liglab.mining.mapred.writables.ItemAndSupportWritable;
import fr.liglab.mining.mapred.writables.SupportAndTransactionWritable;

public class AggregationMapper extends Mapper<IntWritable, SupportAndTransactionWritable, ItemAndSupportWritable, SupportAndTransactionWritable> {

	private final ItemAndSupportWritable keyW = new ItemAndSupportWritable();
	
	@Override
	protected void map(IntWritable key, SupportAndTransactionWritable value, Context context)
			throws IOException, InterruptedException {
		
		keyW.set(key.get(), value.getSupport());
		context.write(this.keyW, value);
	}
}
