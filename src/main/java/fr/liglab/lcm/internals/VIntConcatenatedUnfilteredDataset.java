package fr.liglab.lcm.internals;

import com.google.common.primitives.Ints;

import fr.liglab.lcm.LCM.DontExploreThisBranchException;

public class VIntConcatenatedUnfilteredDataset extends UnfilteredDataset {

	public VIntConcatenatedUnfilteredDataset(IterableDataset parentDataset, int extension, int[] ignoreItems)
			throws DontExploreThisBranchException {
		super(parentDataset, extension, ignoreItems);
	}

	public VIntConcatenatedUnfilteredDataset(UnfilteredDataset upper, int extension, int[] ignoreItems)
			throws DontExploreThisBranchException {
		super(upper, extension, ignoreItems);
	}

	@Override
	public Dataset createUnfilteredDataset(UnfilteredDataset upper, int extension, int[] ignoreItems)
			throws DontExploreThisBranchException {
		return new VIntConcatenatedUnfilteredDataset(upper, extension, ignoreItems);
	}

	@Override
	public Dataset createFilteredDataset(IterableDataset upper, int extension, int[] ignoredItems)
			throws DontExploreThisBranchException {
		return new VIntConcatenatedDataset(upper, extension, Ints.concat(this.ignoreItems, ignoredItems));
	}

}
