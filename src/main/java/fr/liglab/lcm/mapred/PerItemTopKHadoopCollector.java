package fr.liglab.lcm.mapred;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import fr.liglab.lcm.io.PerItemTopKCollector;
import fr.liglab.lcm.mapred.writables.SupportAndTransactionWritable;
import fr.liglab.lcm.mapred.writables.TransactionWritable;
import gnu.trove.procedure.TIntObjectProcedure;

public class PerItemTopKHadoopCollector extends PerItemTopKCollector {
	
	protected final Reducer<IntWritable, TransactionWritable, IntWritable, SupportAndTransactionWritable>.Context context;
	
	public PerItemTopKHadoopCollector(int k, Reducer<IntWritable, TransactionWritable, IntWritable, SupportAndTransactionWritable>.Context currentContext) {
		super(null, k);
		
		this.context = currentContext;
	}
	
	@Override
	public void close() {
		final IntWritable keyW = new IntWritable();
		final SupportAndTransactionWritable valueW = new SupportAndTransactionWritable();
		
		this.topK.forEachEntry(new TIntObjectProcedure<PatternWithFreq[]>() {
			
			public boolean execute(final int item, final PatternWithFreq[] itemTopK) {
				keyW.set(item);
				
				for (int i = 0; i < itemTopK.length && itemTopK[i] != null; i++) {
					
					PatternWithFreq entry = itemTopK[i];
					valueW.set(entry.getSupportCount(), entry.getPattern());
					
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
