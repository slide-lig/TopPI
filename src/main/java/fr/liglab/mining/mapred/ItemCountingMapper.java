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

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import fr.liglab.mining.mapred.writables.ItemAndSupportWritable;
import gnu.trove.iterator.TIntIntIterator;
import gnu.trove.map.TIntIntMap;
import gnu.trove.map.hash.TIntIntHashMap;

public class ItemCountingMapper extends
		Mapper<LongWritable, Text, NullWritable, ItemAndSupportWritable> {
	
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
		final NullWritable nullKey = NullWritable.get();
		final ItemAndSupportWritable valueW = new ItemAndSupportWritable();
		
		TIntIntIterator it = this.combiner.iterator();
		
		while(it.hasNext()) {
			it.advance();
			valueW.set(it.key(), it.value());
			context.write(nullKey, valueW);
		}
		
		this.combiner = null;
	}
}
