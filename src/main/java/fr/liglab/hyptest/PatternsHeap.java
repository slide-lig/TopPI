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
package fr.liglab.hyptest;

import java.util.Arrays;
import java.util.Iterator;

import edu.northwestern.at.utils.math.Factorial;
import fr.liglab.hyptest.math.Fisher;
import fr.liglab.hyptest.math.FishersExactTest;
import fr.liglab.mining.mapred.writables.SupportAndTransactionWritable;

/**
 * Collects top-correlated itemsets for a single item
 * 
 * The statistical test happens here.
 * Don't forget to set PatternsHeap.nbTransactions as soon as possible.
 * 
 * @author kirchgem
 */
public class PatternsHeap {
	
	public static int nbTransactions;
	public static final Fisher tester = new Fisher();
	
	private final Pattern[] heap;
	private int tail = 0;

	/**
	 * We always have maxRejectedSupport < itemSupport < minRejectedSupport
	 */
	public final int itemSupport;

	/**
	 * called "max" because we want to maximize this value
	 */
	private int maxRejectedSupport;
	
	/**
	 * called "min" because we want to minimize this value
	 */
	private int minRejectedSupport;
	
	/**
	 * @param size the "k" in top-k
	 * @param itemSupport support count of the item associated to this heap
	 */
	public PatternsHeap(int size, int itemSupport) {
		this.heap = new Pattern[size];
		this.itemSupport = itemSupport;
		this.resetBounds();
	}
	
	protected void resetBounds() {
		this.maxRejectedSupport = Integer.MIN_VALUE;
		this.minRejectedSupport = Integer.MAX_VALUE;
	}
	
	public int getMaxRejectedSupport() {
		return maxRejectedSupport;
	}
	
	@Override
	public String toString() {
		return this.toString(null);
	}
	
	public String toString(long[] translateItems) {
		StringBuffer sb = new StringBuffer();
		sb.append("\t"+this.itemSupport+"\t");
		if (this.maxRejectedSupport > 0 && this.minRejectedSupport < Integer.MAX_VALUE){
			sb.append(this.maxRejectedSupport+"\t"+minRejectedSupport);
		} else if (this.maxRejectedSupport > 0){
			sb.append(this.maxRejectedSupport+"\t");
		} else if (this.minRejectedSupport < Integer.MAX_VALUE){
			sb.append("\t"+this.minRejectedSupport);
		} else {
			sb.append("\t");
		}
		for(int i=0; i < this.tail; i++) {
			sb.append("\n");
			sb.append(this.heap[i].toString(translateItems));
		}
		return sb.toString();
	}
	
	/**
	 * @param pattern an itemset X
	 * @param patternSupport supportCount(X)
	 * @param unionSupport supportCount(X union item)
	 * @return the highest rejected support ever seen by this heap
	 */
	public int insert(int[] pattern, int patternSupport, int unionSupport) {
		Pattern last;
		int pos;
		
		synchronized(this) {
		if (this.tail == this.heap.length) {
			pos = this.heap.length-1;
			last = this.heap[pos];
			
			if (patternSupport >= this.minRejectedSupport) {
				return this.maxRejectedSupport;
			} else if (patternSupport <= this.maxRejectedSupport) {
				return this.maxRejectedSupport;
			} else if (!betaCheck(patternSupport, last)) {
				if (patternSupport < this.itemSupport) {
					if (patternSupport > this.maxRejectedSupport) {
						this.maxRejectedSupport = patternSupport;
					}
				} else {
					 if (patternSupport < this.minRejectedSupport){
						 this.minRejectedSupport = patternSupport;
					 }
				}
				
				return this.maxRejectedSupport;
			}

		} else {
			pos = this.tail;
			last = null;
		}
		}
		
		long a = tester.getCorrectionFactor(unionSupport, 
				patternSupport - unionSupport, 
				this.itemSupport - unionSupport,
				PatternsHeap.nbTransactions - this.itemSupport - patternSupport + unionSupport);
		
		if (last != null && a < last.a) {
			return this.maxRejectedSupport;
		}
		
		double r = tester.getTailedFisher(a);
		
		synchronized(this) {
		if (this.tail == this.heap.length) {
			pos = this.heap.length-1;
			last = this.heap[pos];
		} else {
			pos = this.tail;
			last = null;
		}
		
		if (last == null || (a > last.a) || (a == last.a && r < last.r)) {
			this.heap[pos] = new Pattern(pattern, patternSupport, unionSupport, a, r);
			if (last == null) {
				this.tail++;
			} else {
				this.resetBounds();
			}
			Arrays.sort(this.heap, 0, this.tail);
		}
		}
		
		return this.maxRejectedSupport;
	}

	/**
	 * the min-p test (aka \beta)
	 * assumes that this heap is full.
	 * @param s_x support count of the pattern to be tested
	 * @param k-th pattern in the heap
	 * @return true if, at this support, we have any chance to enter this heap
	 */
	protected boolean betaCheck(int s_x, Pattern tail) {
		final double a_k = tail.a;
		
		double left;
		if (s_x < this.itemSupport) {
			left = Math.exp(FishersExactTest.ADJUSTMENT_RANGE*a_k
					+Factorial.logFactorial(this.itemSupport)
					+Factorial.logFactorial(PatternsHeap.nbTransactions-s_x)
					-Factorial.logFactorial(PatternsHeap.nbTransactions)
					-Factorial.logFactorial(this.itemSupport-s_x));
		} else {
			left = Math.exp(FishersExactTest.ADJUSTMENT_RANGE*a_k
					+Factorial.logFactorial(s_x)
					+Factorial.logFactorial(PatternsHeap.nbTransactions-this.itemSupport)
					-Factorial.logFactorial(PatternsHeap.nbTransactions)
					-Factorial.logFactorial(s_x-this.itemSupport));
		}
		return (left < tail.r);
	}
	
	/**
	 * the min-p test (aka \beta)
	 * @param s_x support count of the pattern to be tested
	 * @return true if, at this support, we have any chance to enter this heap
	 */
	protected synchronized boolean betaCheck(int s_x) {
		if (this.tail == this.heap.length) {
			Pattern last = this.heap[this.heap.length-1];
			return this.betaCheck(s_x, last);
		} else {
			return true;
		}
	}
	
	public Iterator<SupportAndTransactionWritable> getWritables() {
		return new ToWritable();
	}
	
	private final class ToWritable implements Iterator<SupportAndTransactionWritable> {
		private final SupportAndTransactionWritable writable = new SupportAndTransactionWritable();
		private int i = 0;
		
		@Override
		public boolean hasNext() {
			return this.i < tail;
		}

		@Override
		public SupportAndTransactionWritable next() {
			this.writable.set(heap[this.i].supportWithCorrelated, heap[this.i].pattern);
			this.i++;
			return this.writable;
		}
		
	}
}
