package fr.liglab.lcm.mapred;

import org.apache.hadoop.mapreduce.Partitioner;

import fr.liglab.lcm.mapred.writables.ItemAndSupportWritable;
import fr.liglab.lcm.mapred.writables.TransactionWritable;

/**
 * All patterns involving an item should end to the same reducer
 */
public class AggregationPartitioner extends Partitioner<ItemAndSupportWritable, TransactionWritable> {

	@Override
	public int getPartition(ItemAndSupportWritable key, TransactionWritable value, int numPartitions) {
		return key.getItem() % numPartitions;
	}

}
