package fr.liglab.mining.mapred;

import gnu.trove.iterator.TIntIntIterator;
import gnu.trove.map.TIntIntMap;
import gnu.trove.map.hash.TIntIntHashMap;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ItemBigCountingMapper extends
		Mapper<LongWritable, Text, IntWritable, IntWritable> {
	
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
		final IntWritable keyW = new IntWritable();
		final IntWritable valueW = new IntWritable();
		
		TIntIntIterator it = this.combiner.iterator();
		
		while(it.hasNext()) {
			it.advance();
			keyW.set(it.key());
			valueW.set(it.value());
			context.write(keyW, valueW);
		}
		
		this.combiner = null;
	}
}
