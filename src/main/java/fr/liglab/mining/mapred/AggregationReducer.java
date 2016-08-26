/*
	This file is part of TopPI - see https://github.com/slide-lig/TopPI/
	
	Copyright 2016 Martin Kirchgessner, Vincent Leroy, Alexandre Termier, Sihem Amer-Yahia, Marie-Christine Rousset, Université Grenoble Alpes, LIG, CNRS
	Licensed under the Apache License, Version 2.0 (the "License");
	you may not use this file except in compliance with the License.
	You may obtain a copy of the License at
	 http://www.apache.org/licenses/LICENSE-2.0
	 
	or see the LICENSE.txt file joined with this program.
	Unless required by applicable law or agreed to in writing, software
	distributed under the License is distributed on an "AS IS" BASIS,
	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
	See the License for the specific language governing permissions and
	limitations under the License.
*/
package fr.liglab.mining.mapred;

import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

import fr.liglab.mining.mapred.writables.ItemAndSupportWritable;
import fr.liglab.mining.mapred.writables.SupportAndTransactionWritable;

public class AggregationReducer extends Reducer<ItemAndSupportWritable, SupportAndTransactionWritable, IntWritable, SupportAndTransactionWritable> {
	
	protected final IntWritable keyW = new IntWritable();
	protected final SupportAndTransactionWritable valueW = new SupportAndTransactionWritable();
	
	protected int k,lastCount;
	
	@Override
	protected void setup(Context context) throws java.io.IOException , InterruptedException {
		this.k = context.getConfiguration().getInt(TopPIoverHadoop.KEY_K, 1);
		this.lastCount = 0;
	}
	
	protected void reduce(ItemAndSupportWritable key, java.lang.Iterable<SupportAndTransactionWritable> patterns, Context context)
			throws java.io.IOException, InterruptedException {
		
		if (key.getItem() != this.keyW.get()) {
			this.lastCount = 0;
			this.keyW.set(key.getItem());
		}
		
		if (this.lastCount < this.k) {
			Iterator<SupportAndTransactionWritable> it = patterns.iterator();
			while (it.hasNext() && this.lastCount < this.k) {
				context.write(this.keyW, it.next());
				this.lastCount++;
			}
		}
	}
	
	/**
	 * All patterns involving an item should end to the same reducer
	 */
	public static class AggregationPartitioner extends Partitioner<ItemAndSupportWritable, SupportAndTransactionWritable> {
		
		@Override
		public int getPartition(ItemAndSupportWritable key, SupportAndTransactionWritable value, int numPartitions) {
			return key.getItem() % numPartitions;
		}
	}
}
