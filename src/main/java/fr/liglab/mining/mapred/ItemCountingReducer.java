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
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import fr.liglab.mining.mapred.writables.ItemAndSupportWritable;
import fr.liglab.mining.util.ItemAndBigSupport;
import gnu.trove.iterator.TIntIntIterator;
import gnu.trove.map.TIntIntMap;
import gnu.trove.map.hash.TIntIntHashMap;

public class ItemCountingReducer extends
		Reducer<NullWritable, ItemAndSupportWritable, IntWritable, IntWritable> {
	
	public static final String COUNTERS_GROUP = "ItemCounters";
	public static final String COUNTER_REBASING_MAX_ID = "rebasing-maxId";
	
	protected TIntIntMap itemSupports;
	

	@Override
	protected void setup(Context context) throws java.io.IOException, InterruptedException {
		this.itemSupports = new TIntIntHashMap();
	}
	
	@Override
	protected void reduce(NullWritable key, 
			Iterable<ItemAndSupportWritable> values, Context context)
			throws java.io.IOException, InterruptedException {
		
		Iterator<ItemAndSupportWritable> it = values.iterator();
		
		while (it.hasNext()) {
			ItemAndSupportWritable entry = it.next();
			int sup = entry.getSupport();
			
			this.itemSupports.adjustOrPutValue(entry.getItem(), sup, sup);
		}
	}
	
	@Override
	protected void cleanup(Context context) throws java.io.IOException, InterruptedException {
		final Configuration conf = context.getConfiguration();
		final int minSupport = conf.getInt(TopPIoverHadoop.KEY_MINSUP, 10);
		
		final IntWritable keyW = new IntWritable();
		final IntWritable valueW = new IntWritable();
		final TreeSet<ItemAndBigSupport> heap = new TreeSet<ItemAndBigSupport>();
		
		TIntIntIterator it = this.itemSupports.iterator();
		while(it.hasNext()) {
			it.advance();
			final int support = it.value();
			if (support >= minSupport) {
				heap.add(new ItemAndBigSupport(it.key(), support));
			}
		}
		
		this.itemSupports = null;
		int rebased = 0;
		
		for (ItemAndBigSupport entry : heap) {
			keyW.set(entry.item);
			valueW.set(rebased++);
			context.write(keyW, valueW);
		}
		
		rebased -= 1;
		
		context.getCounter(COUNTERS_GROUP, COUNTER_REBASING_MAX_ID).setValue(rebased);
	}
}
