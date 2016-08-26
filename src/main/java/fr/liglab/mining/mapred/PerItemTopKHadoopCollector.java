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
import java.util.Arrays;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import fr.liglab.mining.internals.FrequentsIterator;
import fr.liglab.mining.io.PerItemTopKCollector;
import fr.liglab.mining.mapred.writables.SupportAndTransactionWritable;
import gnu.trove.iterator.TIntIntIterator;
import gnu.trove.iterator.TIntObjectIterator;
import gnu.trove.map.TIntIntMap;

@SuppressWarnings("rawtypes")
class PerItemTopKHadoopCollector extends PerItemTopKCollector {
	
	private int nbCollected = 0;
	private long lengthCollected = 0;
	
	private final Context context;
	private final IntWritable keyW = new IntWritable();
	private final SupportAndTransactionWritable valueW = new SupportAndTransactionWritable();
	
	public PerItemTopKHadoopCollector(Context c, final int k, final int nbItems, final FrequentsIterator items) {
		super(null, k, nbItems, items);
		this.context = c;
	}

	@Override
	@SuppressWarnings("unchecked")
	public long close() {
		
		TIntObjectIterator<PatternWithFreq[]> entries = this.topK.iterator();
		
		while (entries.hasNext()) {
			entries.advance();
			this.keyW.set(entries.key());
			final PatternWithFreq[] itemTopK = entries.value();
			
			for (int i = 0; i < itemTopK.length; i++) {
				if (itemTopK[i] == null) {
					break;
				} else if (itemTopK[i].isClosed() && itemTopK[i].getPattern().length > 0){
					final int[] pattern = itemTopK[i].getPattern();
					
					if (pattern == null) {
						break;
					}
					
					this.nbCollected++;
					this.lengthCollected += pattern.length;
					this.valueW.set(itemTopK[i].getSupportCount(), pattern);
					
					try {
						this.context.write(this.keyW, this.valueW);
					} catch (IOException e) {
						e.printStackTrace();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}
		}
		
		return this.nbCollected;
	}
	
	@Override
	public int getAveragePatternLength() {
		return (int)(this.lengthCollected / this.nbCollected);
	}
	
	public void writeTopKBounds(MultipleOutputs<?, ?> sideOutputs, String outputName, String path, int minsup) throws IOException, InterruptedException {
		final IntWritable itemW  = new IntWritable();
		final IntWritable boundW = new IntWritable();
		
		TIntObjectIterator<PatternWithFreq[]> it = this.topK.iterator();
		
		while (it.hasNext()) {
			it.advance();
			if (it.value()[this.k - 1] != null) {
				final int supportCount = it.value()[this.k - 1].getSupportCount();
				
				if (supportCount > minsup) {
					itemW.set(it.key());
					boundW.set(supportCount);
					sideOutputs.write(outputName, itemW, boundW, path);
				}
			}
		}
	}

	// note that "close" overloaded by this class takes into account potentially-null patterns
	public void preloadBounds(TIntIntMap perItemBounds) {
		TIntIntIterator iterator = perItemBounds.iterator();
		int[] fakePattern = new int[] { };
		while (iterator.hasNext()) {
			iterator.advance();
			
			final int item = iterator.key();
			PatternWithFreq[] top = this.topK.get(item);
			
			if (top != null) {
				Arrays.fill(top, new PatternWithFreq(iterator.value(), fakePattern, true));
			}
		}
	}
}
