package fr.liglab.mining.mapred;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class MiningMapper extends Mapper<IntWritable, IntWritable, IntWritable, IntWritable> {
	
	private Grouper grouper;
	private IntWritable keyW = new IntWritable();

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		
		Configuration conf = context.getConfiguration();
		int nbGroups = conf.getInt(TopPIoverHadoop.KEY_NBGROUPS, 1);
		int maxId = conf.getInt(TopPIoverHadoop.KEY_REBASING_MAX_ID, 1);
		
		grouper = new Grouper(nbGroups, maxId);
	}
	
	@Override
	protected void map(IntWritable key, IntWritable value, Context context) throws IOException, InterruptedException {
		
		this.keyW.set(this.grouper.getGroupId(value.get()));
		context.write(this.keyW, value);
	}
}
