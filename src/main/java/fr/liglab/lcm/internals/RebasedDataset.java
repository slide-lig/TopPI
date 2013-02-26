package fr.liglab.lcm.internals;

/**
 * Some datasets are said to be rebased : they're internally using a Rebaser 
 * to re-index items such that 0 is the most frequent one.
 * 
 * This interface ensure they provide a way to translate itemsets they output, 
 * you should also use RebaserCollector
 */
public interface RebasedDataset {
	public int[] getReverseMap();
}
