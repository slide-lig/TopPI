package fr.liglab.lcm.mapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import fr.liglab.lcm.mapred.writables.TransactionWritable;
import fr.liglab.lcm.util.ItemsetsFactory;
import fr.liglab.lcm.util.RebasingAndGroupID;
import gnu.trove.iterator.TIntIterator;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;

public class MiningMapper extends Mapper<LongWritable, Text, IntWritable, TransactionWritable> {
	
	protected final ItemsetsFactory transaction = new ItemsetsFactory();
	protected final TIntSet destinations = new TIntHashSet();
	
	protected final IntWritable keyW = new IntWritable();
	protected final TransactionWritable valueW = new TransactionWritable();
	
	protected TIntObjectMap<RebasingAndGroupID> itemsDispatching = null;
	
	protected void setup(Context context)
			throws java.io.IOException ,InterruptedException {
		
		if (this.itemsDispatching == null) {
			Configuration conf = context.getConfiguration();
			
			this.itemsDispatching = DistCache.readItemsDispatching(conf);
		}
	}
	
	@Override
	protected void map(LongWritable key, Text value, Context context) 
			throws java.io.IOException, InterruptedException {
		
		String[] tokens = value.toString().split("\\s+");
		
		for (String token : tokens) {
			int item = Integer.parseInt(token);
			RebasingAndGroupID dispatching = this.itemsDispatching.get(item);
			
			if (dispatching != null) {
				this.transaction.add(dispatching.rebasing);
				this.destinations.add(dispatching.gid);
			}
		}
		
		if (!destinations.isEmpty()) {
			this.valueW.set(this.transaction.get());
			
			TIntIterator it = this.destinations.iterator();
			while (it.hasNext()) {
				this.keyW.set(it.next());
				
				context.write(this.keyW, this.valueW);
			}
			
			this.destinations.clear();
		}
	}
}
