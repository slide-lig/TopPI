package fr.liglab.lcm.internals;

import fr.liglab.lcm.util.ItemsetsFactory;
import gnu.trove.iterator.TIntIterator;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.map.hash.TIntIntHashMap;

import java.util.Arrays;

public class ConcatenatedUnfilteredDataset extends Dataset {
	public static final double FILTERING_THRESHOLD = 0.15;
	
	protected final ConcatenatedDataset parent;
	protected final TIntArrayList tids; // indexes in parent_concatenated
	protected final int coreItem;
	protected final int[] frequentItems;
	protected final int[] ignoreItems; // items known to have a 100% support
	
	/**
	 * builds an unfiltered projection of parentDataset over "extension"
	 */
	public ConcatenatedUnfilteredDataset(ConcatenatedDataset parentDataset, int extension) {
		this.coreItem = extension;
		this.minsup = parentDataset.minsup;
		
		this.parent = parentDataset;
		this.tids = parentDataset.occurrences.get(extension);
		
		this.genSupportCounts();
		this.supportCounts.remove(extension);
		this.genClosureAndFilterCount();
		
		this.ignoreItems = ItemsetsFactory.extend(this.discoveredClosure, extension);
		
		this.frequentItems = this.supportCounts.keys();
		Arrays.sort(this.frequentItems);
	}
	
	public ConcatenatedUnfilteredDataset(ConcatenatedUnfilteredDataset upper, 
			int extension, TIntArrayList extensionTids) {
		
		this.coreItem = extension;
		this.minsup = upper.minsup;
		
		this.parent = upper.parent;
		this.tids = extensionTids;
		
		this.genSupportCounts();
		this.supportCounts.remove(extension);
		
		for (int item : upper.ignoreItems) {
			this.supportCounts.remove(item);
		}
		this.genClosureAndFilterCount();
		
		this.ignoreItems = ItemsetsFactory.extend(discoveredClosure, extension, upper.ignoreItems);
		
		this.frequentItems = this.supportCounts.keys();
		Arrays.sort(this.frequentItems);
	}
	
	/**
	 * requires : this.parent and this.tids
	 */
	private void genSupportCounts() {
		this.supportCounts = new TIntIntHashMap();
		
		TIntIterator iterator = this.tids.iterator();
		while (iterator.hasNext()) {
			int tid = iterator.next();
			int length = this.parent.concatenated[tid];
			for (int i = tid + 1; i <= tid + length; i++) {
				this.supportCounts.adjustOrPutValue(this.parent.concatenated[i], 1,
						1);
			}
		}
	}
	
	@Override
	public int getTransactionsCount() {
		return this.tids.size();
	}

	@Override
	public Dataset getProjection(int extension) {
		// extensionTids = current Tids intersected with extension's occurrences
		TIntArrayList extensionTids = new TIntArrayList(tids);
		extensionTids.retainAll(this.parent.occurrences.get(extension));
		
		double extensionSupport = this.supportCounts.get(extension);
		
		if ((extensionSupport / this.parent.getTransactionsCount()) > FILTERING_THRESHOLD) {
			return new ConcatenatedUnfilteredDataset(this, extension, extensionTids);
		} else {
			return new ConcatenatedDataset(this.parent, extension, extensionTids, this.ignoreItems);
		}
	}

	@Override
	public ExtensionsIterator getCandidatesIterator() {
		return new CandidatesIterator();
	}
	
	public class CandidatesIterator implements ExtensionsIterator {
		
		private int i = 0;
		
		public int[] getSortedFrequents() {
			return frequentItems;
		}

		public int getExtension() {
			if (i < frequentItems.length) {
				return frequentItems[i++];
			} else {
				return -1;
			}
		}
	}
}
