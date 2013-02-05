package fr.liglab.lcm.internals;

import gnu.trove.iterator.TIntIterator;

import java.util.Iterator;

public class InitialDataset extends Dataset {
	
	public InitialDataset(long minimumsupport, Iterator<int[]> transactions) {
		minsup = minimumsupport;
		
		// TODO
	}

	@Override
	public long getTransactionsCount() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int[] getDiscoveredClosureItems() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Dataset getProjection(int extension) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public TIntIterator getCandidatesIterator() {
		// TODO Auto-generated method stub
		return null;
	}

}
