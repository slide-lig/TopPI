package fr.liglab.hyptest.math;

import gnu.trove.map.TObjectDoubleMap;
import gnu.trove.map.TObjectLongMap;
import gnu.trove.map.hash.TObjectDoubleHashMap;
import gnu.trove.map.hash.TObjectLongHashMap;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;

public class Fisher {
	private TObjectDoubleMap<Param> resCache = new TObjectDoubleHashMap<Param>(
			1000, .5f, Double.NaN);
	private TObjectLongMap<Param> factorCache = new TObjectLongHashMap<Param>(
			1000, .5f, Long.MAX_VALUE);
	
	private static final ThreadLocal<Param> reusableParam = new ThreadLocal<Param>() {
		@Override
		protected Param initialValue() {
			return new Param(0, 0, 0, 0);
		}
	};
	
	/**
	 *	@param	a		Frequency for cell(1,1).
	 *	@param	b		Frequency for cell(1,2).
	 *	@param	c		Frequency for cell(2,1).
	 *	@param	d		Frequency for cell(2,2).
	 * @return correction factor (ie. how many times should we add/substract
	 *         ADJUSTMENT_RANGE to have a log(factorials) in
	 *         [-ADJUSTMENT_RANGE/2, ADJUSTMENT_RANGE/2]
	 */
	public long getCorrectionFactor(final int a, final int b, final int c,
			final int d) {
		Param p = reusableParam.get();
		p.set(a, b, c , d);
		long factor;
		synchronized (this.factorCache) {
			factor = this.factorCache.get(p);
		}
		if (factor == Long.MAX_VALUE) {
			factor = FishersExactTest.correctionFactor(a, b, c, d);
			synchronized (this.factorCache) {
				this.factorCache.put(p.clone(), factor);
			}
		}
		return factor;
	}
	
	/**
	 * re-uses the contingency matrix from the previous call to getCorrectionFactor
	 * thread-safe
	 * @param f adjustment factor
	 * @return adjusted p-val
	 */
	public double getTailedFisher(final long f) {
		Param p = reusableParam.get();
		double r;
		synchronized (this.resCache) {
			r = resCache.get(p);
		}
		if (Double.isNaN(r)) {
			r = FishersExactTest.fishersExactTest(p.a, p.b, p.c, p.d, f);
			synchronized (this.resCache) {
				resCache.put(p.clone(), r);
			}
		}
		return r;
	}

	/**
	 *	@param	a		Frequency for cell(1,1).
	 *	@param	b		Frequency for cell(1,2).
	 *	@param	c		Frequency for cell(2,1).
	 *	@param	d		Frequency for cell(2,2).
	 *  @param  f		adjustment factor
	 * @return adjusted p-val
	 */
	public double getTailedFisher(final int a, final int b, final int c,
			final int d, final long f) {
		Param param = reusableParam.get();
		param.set(a, b, c, d);
		return getTailedFisher(f);
	}

	private static class Param implements Cloneable {
		int a;
		int b;
		int c;
		int d;
		
		public Param(final int a, final int b, final int c, final int d) {
			this.a = a;
			this.b = (b<c)?b:c;
			this.c = (b<c)?c:b;
			this.d = d;
		}

		public final void set(int a, int b, int c, int d) {
			this.a = a;
			this.b = b;
			this.c = c;
			this.d = d;
		}

		@Override
		public Param clone() {
			try {
				return (Param) super.clone();
			} catch (CloneNotSupportedException e) {
				e.printStackTrace();
			}
			return null;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + a;
			result = prime * result + b;
			result = prime * result + d;
			result = prime * result + c;
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			Param other = (Param) obj;
			if (a != other.a)
				return false;
			if (b != other.b)
				return false;
			if (d != other.d)
				return false;
			if (c != other.c)
				return false;
			return true;
		}
		
		@Override
		public String toString() {
			return "("+a+","+b+","+c+","+d+")";
		}
	}

	public static final class ScoreAndLine {
		public final long correction;
		public final double score;
		public final String line;

		public ScoreAndLine(long c, double s, String l) {
			this.correction = c;
			this.score = s;
			this.line = l;
		}
	}

	public static void main(String[] args) throws Exception {

		if (args.length != 3) {
			System.err
					.println("USAGE: Fisher CONTINGENCY_FILE NB_TRANSACTIONS KEY_ITEM_SUPPORT");
			System.exit(1);
		}

		final int datasetSize = Integer.parseInt(args[1]);
		final int itemSupport = Integer.parseInt(args[2]);

		Fisher f = new Fisher();
		BufferedReader br = new BufferedReader(new FileReader(args[0]));
		String line;

		ArrayList<ScoreAndLine> result = new ArrayList<ScoreAndLine>();

		while ((line = br.readLine()) != null) {
			if (!line.isEmpty()) {
				String[] sp = line.split("\t");
				int fullSupport = Integer.parseInt(sp[0]);
				int goodSupport = Integer.parseInt(sp[1]);
				int d = datasetSize- itemSupport - fullSupport + goodSupport;
				long factor = f.getCorrectionFactor(goodSupport, fullSupport -goodSupport, itemSupport - goodSupport, d);
				double pVal = f.getTailedFisher(goodSupport, fullSupport -goodSupport, itemSupport - goodSupport, d, factor);
				result.add(new ScoreAndLine(factor, pVal, line));
			}
		}
		br.close();

		ScoreAndLine[] asArray = result.toArray(new ScoreAndLine[0]);
		Arrays.sort(asArray, new Comparator<ScoreAndLine>() {

			@Override
			public int compare(ScoreAndLine a, ScoreAndLine b) {
				int c = Long.compare(b.correction, a.correction);
				if (c == 0) {
					return Double.compare(a.score, b.score);
				} else {
					return c;
				}
			}
		});

		for (ScoreAndLine item : asArray) {
			System.out.println(item.correction + "^^" + item.score + "\t"
					+ item.line);
		}

	}

}
