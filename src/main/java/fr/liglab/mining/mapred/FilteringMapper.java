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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import fr.liglab.mining.mapred.writables.ConcatenatedTransactionsWritable;
import fr.liglab.mining.util.ItemsetsFactory;
import gnu.trove.map.TIntIntMap;

/**
 * outputted records are GroupID => concatenated transactions Transactions will
 * be filtered and rebased according to the global rebasing map
 */
public final class FilteringMapper extends Mapper<LongWritable, Text, NullWritable, ConcatenatedTransactionsWritable> {

	private List<int[]> combined;
	private int combinedLength;
	private TIntIntMap rebasing = null;
	protected final ItemsetsFactory transaction = new ItemsetsFactory();

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		if (this.rebasing == null) {
			this.rebasing = DistCache.readRebasing(DistCache.getCachedFiles(context), context.getConfiguration());
		}

		this.combined = new ArrayList<int[]>();
		this.combinedLength = 0;
	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String[] tokens = value.toString().split("\\s+");

		for (String token : tokens) {
			int item = Integer.parseInt(token);

			if (this.rebasing.containsKey(item)) {
				this.transaction.add(this.rebasing.get(item));
			}
		}

		if (!this.transaction.isEmpty()) {
			int[] transaction = this.transaction.get();

			this.combined.add(transaction);
			this.combinedLength += transaction.length;
		}
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		final ConcatenatedTransactionsWritable valueW = new ConcatenatedTransactionsWritable();
		valueW.set(combined, combinedLength);
		context.write(NullWritable.get(), valueW);
	}
}
