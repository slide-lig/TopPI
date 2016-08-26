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
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import javax.xml.ws.Holder;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import fr.liglab.mining.internals.ExplorationStep;
import fr.liglab.mining.io.FileFilteredReader;
import fr.liglab.mining.mapred.Grouper.SingleGroup;
import fr.liglab.mining.mapred.writables.SupportAndTransactionWritable;
import fr.liglab.mining.util.ProgressWatcherThread;
import gnu.trove.map.TIntIntMap;

public class MiningReducer extends Reducer<IntWritable, IntWritable, IntWritable, SupportAndTransactionWritable> {

	private int[] reverseRebasing;
	private MultipleOutputs<IntWritable, SupportAndTransactionWritable> sideOutputs = null;
	private boolean manyItems;
	private String marker;
	private int minsup;
	private int nbGroups;
	private int maxItemId;
	private int k;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		this.reverseRebasing = DistCache.readReverseRebasing(DistCache.getCachedFiles(context), conf);

		if (conf.get(MinerWrapper.KEY_BOUNDS_PATH) != null) {
			this.sideOutputs = new MultipleOutputs<IntWritable, SupportAndTransactionWritable>(context);
		}
		this.manyItems = conf.getBoolean(TopPIoverHadoop.KEY_MANY_ITEMS_MODE, false);
		if (manyItems) {
			this.marker = TopPIoverHadoop.FILTERED_DIRNAME;
		} else {
			this.marker = conf.get(TopPIoverHadoop.KEY_INPUT);
			String[] sp = marker.split("/");
			if (sp.length > 2) {
				this.marker = sp[sp.length - 1];
			}
		}
		this.minsup = conf.getInt(TopPIoverHadoop.KEY_MINSUP, 10);
		this.nbGroups = conf.getInt(TopPIoverHadoop.KEY_NBGROUPS, 1);
		this.maxItemId = conf.getInt(TopPIoverHadoop.KEY_REBASING_MAX_ID, 1);
		this.k = conf.getInt(TopPIoverHadoop.KEY_K, 1);
	}

	@Override
	protected void reduce(IntWritable gidW, Iterable<IntWritable> itemsW, Context context) throws IOException,
			InterruptedException {
		Configuration conf = context.getConfiguration();
		int gid = gidW.get();

		ProgressWatcherThread coucou = new ProgressWatcherThread();
		coucou.setHadoopContext(context);
		coucou.start();

		List<URI> cached = new ArrayList<URI>();
		for (URI path : DistCache.getCachedFiles(context)) {
			if (path.toString().contains(marker)) {
				cached.add(path);
			}
		}

		SingleGroup filter = new SingleGroup(nbGroups, maxItemId, gid);
		Holder<int[]> renaming = new Holder<int[]>();
		ExplorationStep initState = null;
		if (manyItems) {
			FilteredDatasetsReader reader = new FilteredDatasetsReader(cached, conf, filter);
			initState = new ExplorationStep(minsup, reader, maxItemId, this.reverseRebasing, renaming, k);
		} else {
			if (cached.size() > 1) {
				throw new RuntimeException("Without 'many items mode' a single input file is expected in the distcache");
			}
			URI path = cached.get(0);
			TIntIntMap rebasing = DistCache.readRebasing(DistCache.getCachedFiles(context), conf);
			FileFilteredReader reader = new FileFilteredReader(path, rebasing, filter);
			initState = new ExplorationStep(minsup, reader, maxItemId, this.reverseRebasing, renaming, k);
		}

		System.err.println("GROUP " + gid + ": " + initState.counters.toString());

		coucou.interrupt();
		coucou = null;

		MinerWrapper.mining(gid, initState, context, this.sideOutputs, this.reverseRebasing, renaming);
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		if (this.sideOutputs != null) {
			this.sideOutputs.close();
		}
	}
}
