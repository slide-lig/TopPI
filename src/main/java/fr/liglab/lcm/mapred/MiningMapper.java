package fr.liglab.lcm.mapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import fr.liglab.lcm.mapred.groupers.Grouper;
import fr.liglab.lcm.mapred.writables.TransactionWritable;
import fr.liglab.lcm.util.ItemsetsFactory;
import gnu.trove.iterator.TIntIterator;
import gnu.trove.map.TIntIntMap;
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;

public class MiningMapper extends Mapper<LongWritable, Text, IntWritable, TransactionWritable> {
	
	protected final ItemsetsFactory transaction = new ItemsetsFactory();
	protected final TIntSet destinations = new TIntHashSet();
	
	protected final IntWritable keyW = new IntWritable();
	protected final TransactionWritable valueW = new TransactionWritable();
	
	protected TIntIntMap itemsRebasing = null;
	protected Grouper grouper;
	
	protected void setup(Context context)
			throws java.io.IOException ,InterruptedException {
		
		if (this.itemsRebasing == null) {
			Configuration conf = context.getConfiguration();
			this.itemsRebasing = DistCache.readItemsRebasing(conf);
			this.grouper = Grouper.factory(conf);
		}
	}
	
	@Override
	protected void map(LongWritable key, Text value, Context context) 
			throws java.io.IOException, InterruptedException {
		
		String[] tokens = value.toString().split("\\s+");
		
		for (String token : tokens) {
			int item = Integer.parseInt(token);
			
			if (this.itemsRebasing.containsKey(item)) {
				int rebased = this.itemsRebasing.get(item);
				this.transaction.add(rebased);
				this.destinations.add(this.grouper.getGid(rebased));
			}
		}
		
		if (!destinations.isEmpty()) {
			this.valueW.set(this.transaction.get());
			
			TIntIterator it = this.destinations.iterator();
			while (it.hasNext()) {
				int gid = it.next();
				if (gid >= 0) {
					this.keyW.set(gid);
					
					context.write(this.keyW, this.valueW);
				}
			}
			
			this.destinations.clear();
		}
	}
}
