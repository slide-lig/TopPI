package fr.liglab.lcm.mapred;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import fr.liglab.lcm.io.PerItemTopKCollector;
import fr.liglab.lcm.mapred.writables.ItemAndSupportWritable;
import fr.liglab.lcm.mapred.writables.TransactionWritable;
import gnu.trove.procedure.TIntObjectProcedure;

public class PerItemTopKHadoopCollector extends PerItemTopKCollector {
	
	protected final Reducer<IntWritable, TransactionWritable, ItemAndSupportWritable, TransactionWritable>.Context context;
	
	public PerItemTopKHadoopCollector(int k, Reducer<IntWritable, TransactionWritable, ItemAndSupportWritable, TransactionWritable>.Context currentContext) {
		super(null, k);
		
		this.context = currentContext;
	}
	
	@Override
	public void close() {
		final ItemAndSupportWritable keyW = new ItemAndSupportWritable();
		final TransactionWritable valueW = new TransactionWritable();
		
		this.topK.forEachEntry(new TIntObjectProcedure<PatternWithFreq[]>() {
			
			public boolean execute(final int item, final PatternWithFreq[] itemTopK) {
				keyW.setItem(item);
				
				for (int i = 0; i < itemTopK.length && itemTopK[i] != null; i++) {
					
					PatternWithFreq entry = itemTopK[i];
					
					keyW.setSupport(entry.getSupportCount());
					valueW.set(entry.getPattern());
					
					try {
						context.write(keyW, valueW);
					} catch (Exception e) {
						e.printStackTrace();
						return false;
					}
				}
				
				return true;
			}
		});
	}
}
