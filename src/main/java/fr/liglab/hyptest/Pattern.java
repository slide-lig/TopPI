package fr.liglab.hyptest;

import java.util.Arrays;

/**
 * POJO representing an entry in a patterns heap
 * should be sorted by (a DESC, r ASC) - see compareTo
 * @author kirchgem
 */
public class Pattern implements Comparable<Pattern> {
	/**
	 * The itemset
	 */
	protected int[] pattern = null;
	
	/**
	 * support count of "pattern" alone
	 */
	public final int supportCount;
	
	/**
	 * support count of "pattern"+the correlated item (whose ID is stored by 
	 * the class holding the Pattern collection)
	 */
	public final int supportWithCorrelated;
	
	/**
	 * First half of the correlation score - the higher a, the more correlated 
	 */
	public final long a;
	
	/**
	 * Second half of the correlation score - the lower r, the more correlated
	 */
	public final double r;

	/**
	 * @param p the itemset
	 * @param s itemset's support
	 * @param s_i support count of [itemset U correlatedItem]
	 * @param adjustment score's adjustment factor
	 * @param score
	 */
	public Pattern(int[] p, int s, int s_i, long adjustment, double score) {
		this.pattern = p;
		this.supportCount = s;
		this.supportWithCorrelated = s_i;
		this.a = adjustment;
		this.r = score;
	}
	
	public int[] getPattern() {
		return this.pattern;
	}
	
	@Override
	public int compareTo(Pattern o) {
		final int onA = Long.compare(o.a, this.a);
		if (onA == 0) {
			return Double.compare(this.r, o.r);
		} else {
			return onA;
		}
	}
	
	@Override
	public String toString() {
		return this.toString(null);
	}
	
	public String toString(long[] translateItems) {
		String p;
		if (translateItems == null) {
			p = this.patternToString();
		} else {
			p = this.patternToString(translateItems);
		}
		return this.supportCount + "\t" + this.supportWithCorrelated + "\t"+
				this.a + "\t" + this.r + "\t" + p;
	}
	
	public String patternToString() {
		StringBuilder sb = new StringBuilder();
		boolean sep = false;
		for (int i : this.pattern) {
			if (sep) {
				sb.append(' ');
			}
			sb.append(i);
			sep = true;
		}
		return sb.toString();
	}
	
	public String patternToString(long[] translateItems) {
		StringBuilder sb = new StringBuilder();
		boolean sep = false;
		for (int i : this.pattern) {
			if (sep) {
				sb.append(' ');
			}
			sb.append(translateItems[i]);
			sep = true;
		}
		return sb.toString();
	}
	
	@Override
	public int hashCode() {
		return Integer.hashCode(supportCount) + 
				Integer.hashCode(supportWithCorrelated) + 
				Arrays.hashCode(this.pattern);
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Pattern other = (Pattern) obj;
		if (this.supportCount != other.supportCount)
			return false;
		if (this.supportWithCorrelated != other.supportWithCorrelated)
			return false;
		if (this.a != other.a || this.r != other.r)
			return false;
		if (!Arrays.equals(this.pattern, other.pattern))
			return false;
		return true;
	}
}
