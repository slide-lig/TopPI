package fr.liglab.lcm.mapred;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import fr.liglab.lcm.io.PatternsCollector;
import fr.liglab.lcm.mapred.writables.ItemAndSupportWritable;
import fr.liglab.lcm.mapred.writables.TransactionWritable;

public class HadoopCollector extends PatternsCollector {

	protected final Reducer<IntWritable, TransactionWritable, ItemAndSupportWritable, TransactionWritable>.Context context;
	protected final ItemAndSupportWritable keyW = new ItemAndSupportWritable();
	protected final TransactionWritable valueW = new TransactionWritable();

	protected boolean error = false;

	public HadoopCollector(
			Reducer<IntWritable, TransactionWritable, ItemAndSupportWritable, TransactionWritable>.Context currentContext) {
		this.context = currentContext;
	}
	
	@Override
	public void collect(int support, int[] pattern) {
		
		this.keyW.set(0, support);
		this.valueW.set(pattern);
		
		try {
			this.context.write(this.keyW, this.valueW);

		} catch (IOException e) {
			e.printStackTrace();
			error = true;
		} catch (InterruptedException e) {
			e.printStackTrace();
			error = true;
		}
	}

	@Override
	public void close() {

	}
}
