package fr.liglab.lcm.mapred;

import org.apache.hadoop.mapreduce.Partitioner;

import fr.liglab.lcm.mapred.writables.ItemAndSupportWritable;
import fr.liglab.lcm.mapred.writables.SupportAndTransactionWritable;

/**
 * All patterns involving an item should end to the same reducer
 */
public class AggregationPartitioner extends Partitioner<ItemAndSupportWritable, SupportAndTransactionWritable> {

	@Override
	public int getPartition(ItemAndSupportWritable key, SupportAndTransactionWritable value, int numPartitions) {
		return key.getItem() % numPartitions;
	}

}
