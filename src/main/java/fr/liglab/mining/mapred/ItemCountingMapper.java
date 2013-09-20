package fr.liglab.mining.mapred;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import fr.liglab.mining.mapred.writables.ItemAndSupportWritable;
import gnu.trove.iterator.TIntIntIterator;
import gnu.trove.map.TIntIntMap;
import gnu.trove.map.hash.TIntIntHashMap;

public class ItemCountingMapper extends
		Mapper<LongWritable, Text, NullWritable, ItemAndSupportWritable> {
	
	protected TIntIntMap combiner;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		
		this.combiner = new TIntIntHashMap();
	}
	
	@Override
	protected void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {
		
		String[] tokens = value.toString().split("\\s+");
		
		for (String token : tokens) {
			int item = Integer.parseInt(token);
			this.combiner.adjustOrPutValue(item, 1, 1);
		}
	}
	
	@Override
	protected void cleanup(final Context context) throws IOException, InterruptedException {
		final NullWritable nullKey = NullWritable.get();
		final ItemAndSupportWritable valueW = new ItemAndSupportWritable();
		
		TIntIntIterator it = this.combiner.iterator();
		
		while(it.hasNext()) {
			it.advance();
			valueW.set(it.key(), it.value());
			context.write(nullKey, valueW);
		}
		
		this.combiner = null;
	}
}
