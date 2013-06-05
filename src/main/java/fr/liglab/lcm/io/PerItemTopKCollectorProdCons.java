package fr.liglab.lcm.io;

import fr.liglab.lcm.PLCM.PLCMCounters;
import fr.liglab.lcm.internals.ExplorationStep;
import fr.liglab.lcm.internals.FrequentsIterator;
import fr.liglab.lcm.internals.Selector;
import gnu.trove.iterator.TIntObjectIterator;
import gnu.trove.map.TIntIntMap;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntIntHashMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.procedure.TIntObjectProcedure;

import java.util.Arrays;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Semaphore;

/**
 * Wraps a collector. As a (stateful) Selector it will limit exploration to
 * top-k-per-items patterns.
 * 
 * Thread-safe and initialized with known frequent items.
 */
public class PerItemTopKCollectorProdCons implements PatternsCollector {

	private final PatternsCollector decorated;
	protected final int k;
	protected final TIntObjectMap<PatternWithFreq[]> topK;
	private BlockingQueue<PatternWithFreq> queue;
	private Thread consumerThread;
	private Semaphore consumeComplete = new Semaphore(0);

	public PerItemTopKCollectorProdCons(final PatternsCollector follower, final int k, final int nbItems,
			final FrequentsIterator items) {

		this.decorated = follower;
		this.k = k;
		this.topK = new TIntObjectHashMap<PatternWithFreq[]>(nbItems);

		for (int item = items.next(); item != -1; item = items.next()) {
			this.topK.put(item, new PatternWithFreq[k]);
		}
		this.queue = new ArrayBlockingQueue<PatternWithFreq>(500);
		this.consumerThread = new ConsThread();
		this.consumerThread.start();
	}

	public void collect(final int support, final int[] pattern) {
		this.queue.add(new PatternWithFreq(support, pattern));
	}

	protected void insertPatternInTop(PatternWithFreq pwf, int item) {
		PatternWithFreq[] itemTopK = this.topK.get(item);

		if (itemTopK == null) {
			throw new RuntimeException("item not initialized " + item);
		} else {
			updateTop(pwf, itemTopK);
		}
	}

	private void updateTop(PatternWithFreq pwf, PatternWithFreq[] itemTopK) {
		// we do not have k patterns for this item yet
		if (itemTopK[this.k - 1] == null) {
			// find the position of the last null entry
			int lastNull = k - 1;
			while (lastNull > 0 && itemTopK[lastNull - 1] == null) {
				lastNull--;
			}
			// now compare with the valid entries to adjust position
			int newPosition = lastNull;
			while (newPosition >= 1) {
				if (itemTopK[newPosition - 1].getSupportCount() < pwf.getSupportCount()) {
					newPosition--;
				} else {
					break;
				}
			}
			// make room for the new pattern
			for (int i = lastNull; i > newPosition; i--) {
				itemTopK[i] = itemTopK[i - 1];
			}
			// insert the new pattern where previously computed
			itemTopK[newPosition] = pwf;
		} else
		// the support of the new pattern is higher than the kth previously
		// known
		if (itemTopK[this.k - 1].getSupportCount() < pwf.getSupportCount()) {
			// find where the new pattern is going to be inserted in the
			// sorted topk list
			int newPosition = k - 1;
			while (newPosition >= 1) {
				if (itemTopK[newPosition - 1].getSupportCount() < pwf.getSupportCount()) {
					newPosition--;
				} else {
					break;
				}
			}
			// make room for the new pattern, evicting the one at the end
			for (int i = this.k - 1; i > newPosition; i--) {
				itemTopK[i] = itemTopK[i - 1];
			}
			// insert the new pattern where previously computed
			itemTopK[newPosition] = pwf;
		}
		// else not in top k for this item, do nothing
	}

	public long close() {
		this.consumerThread.interrupt();
		try {
			this.consumeComplete.acquire();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		// we do not want deduplication anymore
		// final Set<PatternWithFreq> dedup = new HashSet<PatternWithFreq>();
		for (final PatternWithFreq[] itemTopK : this.topK.valueCollection()) {
			for (int i = 0; i < itemTopK.length; i++) {
				if (itemTopK[i] == null) {
					break;
				} else {
					// if (dedup.add(itemTopK[i])) {
					this.decorated.collect(itemTopK[i].getSupportCount(), itemTopK[i].getPattern());
					// }
				}
			}
		}

		return this.decorated.close();
	}

	protected int getBound(final int item) {
		final PatternWithFreq[] itemTopK = this.topK.get(item);
		// itemTopK == null should never happen in theory, as
		// currentPattern should be in there at least
		if (itemTopK == null || itemTopK[this.k - 1] == null) {
			return -1;
		} else {
			return itemTopK[this.k - 1].getSupportCount();
		}
	}

	public TIntIntMap getTopKBounds() {
		final TIntIntHashMap bounds = new TIntIntHashMap(this.topK.size());
		TIntObjectIterator<PatternWithFreq[]> it = this.topK.iterator();
		while (it.hasNext()) {
			it.advance();
			int bound = 0;
			if (it.value()[this.k - 1] != null) {
				bound = it.value()[this.k - 1].supportCount;
			}
			bounds.put(it.key(), bound);
		}
		return bounds;
	}

	@Override
	public String toString() {
		final StringBuilder sb = new StringBuilder();
		this.topK.forEachEntry(new TIntObjectProcedure<PatternWithFreq[]>() {

			public boolean execute(int key, PatternWithFreq[] value) {
				sb.append("item " + key + " patterns");
				for (int i = 0; i < value.length; i++) {
					if (value[i] == null) {
						break;
					} else {
						sb.append(" " + value[i]);
					}
				}
				sb.append("\n");
				return true;
			}
		});
		return sb.toString();
	}

	private final class ConsThread extends Thread {

		@Override
		public void run() {
			boolean interrupted = false;
			while (!interrupted) {
				PatternWithFreq p = null;
				try {
					p = queue.take();
				} catch (InterruptedException e) {
					interrupted = true;
				}
				if (p != null) {
					for (int item : p.pattern) {
						insertPatternInTop(p, item);
					}
				}
			}
			while (!queue.isEmpty()) {
				PatternWithFreq p = null;
				try {
					p = queue.take();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				if (p != null) {
					for (int item : p.pattern) {
						insertPatternInTop(p, item);
					}
				}
			}
			consumeComplete.release();
		}

	}

	public static final class PatternWithFreq {
		private final int supportCount;
		private final int[] pattern;

		public PatternWithFreq(final int supportCount, final int[] pattern) {
			super();
			this.supportCount = supportCount;
			this.pattern = pattern;
		}

		public final int getSupportCount() {
			return supportCount;
		}

		public final int[] getPattern() {
			return pattern;
		}

		@Override
		public String toString() {
			return "[supportCount=" + supportCount + ", pattern=" + Arrays.toString(pattern) + "]";
		}

		@Override
		public int hashCode() {
			return Arrays.hashCode(pattern);
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			PatternWithFreq other = (PatternWithFreq) obj;
			if (supportCount != other.supportCount)
				return false;
			if (!Arrays.equals(pattern, other.pattern))
				return false;
			return true;
		}

	}

	public Selector asSelector() {
		return new ExplorationLimiter(null);
	}

	protected final class ExplorationLimiter extends Selector {

		private int previousItem = -1;
		private int previousResult = -1;
		private int validUntuil = Integer.MAX_VALUE;

		ExplorationLimiter(Selector follower) {
			super(follower);
		}

		@Override
		protected PLCMCounters getCountersKey() {
			return PLCMCounters.TopKRejections;
		}

		private synchronized void updatePrevious(final int i, final int r, int v) {
			if (i > this.previousItem) {
				this.previousItem = i;
				this.previousResult = r;
				this.validUntuil = v;
			}
		}

		@Override
		protected boolean allowExploration(int extension, ExplorationStep state) throws WrongFirstParentException {

			int localPreviousItem, localPreviousResult, localValidUntil;
			synchronized (this) {
				localPreviousItem = this.previousItem;
				localPreviousResult = this.previousResult;
				localValidUntil = this.validUntuil;
			}

			int[] reverseRenaming = state.counters.getReverseRenaming();
			int[] supports = state.counters.supportCounts;
			int extensionSupport = supports[extension];
			final int maxCandidate = state.counters.getMaxCandidate();

			boolean shortcut = localValidUntil > extension && localPreviousResult >= extensionSupport;

			if (getBound(reverseRenaming[extension]) < extensionSupport) {
				return true;
			}

			if (!shortcut) {
				localPreviousResult = Integer.MAX_VALUE;
				localValidUntil = Integer.MAX_VALUE;
				for (int i : state.pattern) {
					int bound = getBound(i);
					if (bound < extensionSupport) {
						return true;
					}
					localPreviousResult = Math.min(localPreviousResult, bound);
				}
			}

			FrequentsIterator it;
			if (shortcut) {
				it = state.counters.getLocalFrequentsIterator(localPreviousItem + 1, extension);
			} else {
				it = state.counters.getLocalFrequentsIterator(0, extension);
			}

			for (int i = it.next(); i >= 0; i = it.next()) {
				final int bound = getBound(reverseRenaming[i]);
				if (bound < Math.min(extensionSupport, supports[i])) {
					int firstParent = state.getFailedFPTest(i);

					if (firstParent <= extension) {
						this.updatePrevious(localPreviousItem, localPreviousResult, localValidUntil);
						return true;
					} else if (firstParent < maxCandidate) {
						localValidUntil = Math.min(localValidUntil, firstParent);
					}
				} else {
					localPreviousItem = i;
					localPreviousResult = Math.min(localPreviousResult, bound);
				}
			}

			this.updatePrevious(localPreviousItem, localPreviousResult, localValidUntil);

			return false;
		}

		@Override
		protected Selector copy(Selector newNext) {
			return new ExplorationLimiter(newNext);
		}
	}
}
