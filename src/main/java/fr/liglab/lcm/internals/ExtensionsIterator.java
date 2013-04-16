package fr.liglab.lcm.internals;

import fr.liglab.lcm.LCM.DontExploreThisBranchException;

public interface ExtensionsIterator {
	public int[] getSortedFrequents();

	public int getExtension() throws DontExploreThisBranchException;

}
