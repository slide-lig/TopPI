package fr.liglab.lcm.mapred;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import fr.liglab.lcm.mapred.writables.ItemAndSupportWritable;
import gnu.trove.map.TIntIntMap;
import gnu.trove.map.hash.TIntIntHashMap;
import gnu.trove.procedure.TIntIntProcedure;

public class ItemCountingMapper extends
		Mapper<LongWritable, Text, NullWritable, ItemAndSupportWritable> {
	
	protected TIntIntMap combiner;
	
	protected final NullWritable nullKey = NullWritable.get();
	protected final ItemAndSupportWritable valueW = new ItemAndSupportWritable();

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
		
		this.combiner.forEachEntry(new TIntIntProcedure() {
			
			public boolean execute(int item, int support) {
				valueW.set(item, support);
				
				try {
					context.write(nullKey, valueW);
					
				} catch (IOException e) {
					e.printStackTrace();
					return false;
				} catch (InterruptedException e) {
					e.printStackTrace();
					return false;
				}
				return true;
			}
		});
		
		this.combiner = null;
	}
}
