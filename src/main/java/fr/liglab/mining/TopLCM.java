package fr.liglab.mining;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.omg.CORBA.IntHolder;

import fr.liglab.mining.CountersHandler.TopLCMCounters;
import fr.liglab.mining.internals.Counters;
import fr.liglab.mining.internals.ExplorationStep;
import fr.liglab.mining.io.PerItemTopKCollector;
import fr.liglab.mining.util.ProgressWatcherThread;


public class TopLCM {
	final List<TopLCMThread> threads;
	protected ProgressWatcherThread progressWatch;

	PerItemTopKCollector collector;

	final long[] globalCounters;

	public TopLCM(PerItemTopKCollector patternsCollector, int nbThreads) {
		this(patternsCollector, nbThreads, false);
	}

	public TopLCM(PerItemTopKCollector patternsCollector, int nbThreads, boolean launchProgressWatch) {
		if (nbThreads < 1) {
			throw new IllegalArgumentException("nbThreads has to be > 0, given " + nbThreads);
		}
		this.collector = patternsCollector;
		this.threads = new ArrayList<TopLCMThread>(nbThreads);
		PreparedJobs pj = new PreparedJobs();
		for (int i = 0; i < nbThreads; i++) {
			this.threads.add(new TopLCMThread(pj));
		}

		this.globalCounters = new long[TopLCMCounters.values().length];
		this.progressWatch = launchProgressWatch ? new ProgressWatcherThread() : null;
	}

	/**
	 * Initial invocation for common folks
	 */
	public final void lcm(final ExplorationStep initState) {
		ExecutorService pool = Executors.newFixedThreadPool(this.threads.size());
		this.lcm(initState, pool);
		pool.shutdown();
	}

	/**
	 * Initial invocation for the hard to schedule
	 */
	public final void lcm(final ExplorationStep initState, ExecutorService pool) {
		if (initState.counters.getPattern().length > 0) {
			collector.collect(initState.counters.getTransactionsCount(), initState.counters.getPattern());
		}

		List<Future<?>> running = new ArrayList<Future<?>>(this.threads.size());

		for (TopLCMThread t : this.threads) {
			t.init(initState);
			running.add(pool.submit(t));
		}

		if (this.progressWatch != null) {
			this.progressWatch.setStartersIterator(initState.candidates);
			this.progressWatch.start();
		}

		for (Future<?> t : running) {
			try {
				t.get();
			} catch (InterruptedException e) {
				e.printStackTrace();
				throw new RuntimeException(e);
			} catch (ExecutionException e) {
				e.printStackTrace();
				throw new RuntimeException(e);
			}
		}

		Arrays.fill(this.globalCounters, 0);

		for (TopLCMThread t : this.threads) {
			for (int i = 0; i < t.counters.length; i++) {
				this.globalCounters[i] += t.counters[i];
			}
		}

		if (this.progressWatch != null) {
			this.progressWatch.interrupt();
		}
	}

	public Map<TopLCMCounters, Long> getCounters() {
		HashMap<TopLCMCounters, Long> map = new HashMap<TopLCMCounters, Long>();

		TopLCMCounters[] counters = TopLCMCounters.values();

		for (int i = 0; i < this.globalCounters.length; i++) {
			map.put(counters[i], this.globalCounters[i]);
		}

		return map;
	}

	public String toString(Map<String, Long> additionalCounters) {
		StringBuilder builder = new StringBuilder();

		builder.append("{\"name\":\"TopLCM\", \"threads\":");
		builder.append(this.threads.size());

		TopLCMCounters[] counters = TopLCMCounters.values();

		for (int i = 0; i < this.globalCounters.length; i++) {
			TopLCMCounters counter = counters[i];

			builder.append(", \"");
			builder.append(counter.toString());
			builder.append("\":");
			builder.append(this.globalCounters[i]);
		}

		if (additionalCounters != null) {
			for (Entry<String, Long> entry : additionalCounters.entrySet()) {
				builder.append(", \"");
				builder.append(entry.getKey());
				builder.append("\":");
				builder.append(entry.getValue());
			}
		}

		builder.append('}');

		return builder.toString();
	}

	public String toString() {
		return this.toString(null);
	}

	ExplorationStep stealJob(TopLCMThread thief) {
		// here we need to readlock because the owner thread can write
		for (TopLCMThread victim : this.threads) {
			if (victim != thief) {
				ExplorationStep e = stealJob(thief, victim);
				if (e != null) {
					return e;
				}
			}
		}
		return null;
	}

	ExplorationStep stealJob(TopLCMThread thief, TopLCMThread victim) {
		victim.lock.readLock().lock();
		for (int stealPos = 0; stealPos < victim.stackedJobs.size(); stealPos++) {
			ExplorationStep sj = victim.stackedJobs.get(stealPos);
			ExplorationStep next = sj.next(this.collector);

			if (next != null) {
				thief.stackState(sj);
				victim.lock.readLock().unlock();
				return next;
			}
		}
		victim.lock.readLock().unlock();
		return null;
	}

	private static class CandidateCounters implements Comparable<CandidateCounters> {
		private final int candidate;
		private final Counters counters;
		private final int newCandidateBound;

		private CandidateCounters(int item, Counters counters, int newCandidateBound) {
			super();
			this.candidate = item;
			this.counters = counters;
			this.newCandidateBound = newCandidateBound;
		}

		public final int getCandidate() {
			return candidate;
		}

		public final Counters getCounters() {
			return counters;
		}

		@Override
		public int compareTo(CandidateCounters ic) {
			return this.candidate - ic.candidate;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + candidate;
			return result;
		}

		public final int getNewCandidateBound() {
			return newCandidateBound;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			CandidateCounters other = (CandidateCounters) obj;
			if (candidate != other.candidate)
				return false;
			return true;
		}

	}

	private static class StopPreparingJobsException extends Exception {

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

	}

	private static class StopResumingJobsException extends Exception {

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

	}

	private static class PreparedJobs {
		private Queue<CandidateCounters> stackedEs;
		private int nextConsumable;
		private int minBoundToNextConsumable;

		public PreparedJobs() {
			this.stackedEs = new PriorityQueue<TopLCM.CandidateCounters>();
			this.minBoundToNextConsumable = Integer.MAX_VALUE;
		}

		public synchronized CandidateCounters getTask(IntHolder boundHolder) throws StopPreparingJobsException,
				StopResumingJobsException {
			while (!stackedEs.isEmpty() && stackedEs.peek().getCandidate() == this.nextConsumable) {
				this.nextConsumable++;
				CandidateCounters next = stackedEs.poll();
				if (next.getNewCandidateBound() > 0) {
					this.minBoundToNextConsumable = Math
							.min(this.minBoundToNextConsumable, next.getNewCandidateBound());
				}
				if (next.counters != null) {
					boundHolder.value = this.minBoundToNextConsumable;
					return next;
				}
			}
			if (this.nextConsumable >= ExplorationStep.INSERT_UNCLOSED_UP_TO_ITEM) {
				if (this.stackedEs.isEmpty()) {
					throw new StopResumingJobsException();
				} else {
					throw new StopPreparingJobsException();
				}
			}
			boundHolder.value = this.minBoundToNextConsumable;
			return null;
		}

		public synchronized void pushTask(CandidateCounters t) {
			this.stackedEs.add(t);
		}

	}

	public class TopLCMThread implements Runnable {
		private long[] counters = null;
		private PreparedJobs preparedJobs;
		final ReadWriteLock lock;
		final List<ExplorationStep> stackedJobs;
		final IntHolder candidateHolder = new IntHolder();
		final IntHolder boundHolder = new IntHolder();
		private ExplorationStep rootState;

		public TopLCMThread(PreparedJobs preparedJobs) {
			this.stackedJobs = new ArrayList<ExplorationStep>();
			this.lock = new ReentrantReadWriteLock();
			this.preparedJobs = preparedJobs;
		}

		public void init(ExplorationStep initState) {
			this.stackState(initState);
			this.rootState = initState;
		}

		@Override
		public void run() {
			// no need to readlock, this thread is the only one that can do
			// writes
			boolean exit = false;
			boolean prepareJobs = true;
			boolean resumeJobs = true;
			while (!exit) {
				if (!this.stackedJobs.isEmpty()) {
					if (resumeJobs && this.stackedJobs.size() == 1) {
						CandidateCounters iex = null;
						try {
							iex = this.preparedJobs.getTask(this.boundHolder);
						} catch (StopPreparingJobsException e) {
							prepareJobs = false;
						} catch (StopResumingJobsException e) {
							prepareJobs = false;
							resumeJobs = false;
						}
						if (iex != null) {
							this.stackState(this.rootState.resumeExploration(iex.getCounters(), iex.getCandidate(),
									collector, this.boundHolder.value));
							continue;
						}
					}
					ExplorationStep sj = null;
					sj = this.stackedJobs.get(this.stackedJobs.size() - 1);
					if (prepareJobs && this.stackedJobs.size() == 1) {
						Counters preprocessed = sj.nextPreprocessed(collector, this.candidateHolder, this.boundHolder);
						if (this.candidateHolder.value == -1) {
							this.lock.writeLock().lock();
							this.stackedJobs.remove(this.stackedJobs.size() - 1);
							this.lock.writeLock().unlock();
						} else {
							this.preparedJobs.pushTask(new CandidateCounters(this.candidateHolder.value, preprocessed,
									this.boundHolder.value));
						}
					} else {
						ExplorationStep extended = sj.next(collector);
						// iterator is finished, remove it from the stack
						if (extended == null) {
							this.lock.writeLock().lock();
							this.stackedJobs.remove(this.stackedJobs.size() - 1);
							this.lock.writeLock().unlock();
						} else {
							this.stackState(extended);
						}
					}
				} else { // our list was empty, we should steal from another
							// thread
					prepareJobs = false;
					ExplorationStep stolj = stealJob(this);
					if (stolj == null) {
						exit = true;
					} else {
						stackState(stolj);
					}
				}
			}
			this.counters = CountersHandler.getAll();
		}

		private void stackState(ExplorationStep state) {
			CountersHandler.increment(TopLCMCounters.PatternsTraversed);
			this.lock.writeLock().lock();
			this.stackedJobs.add(state);
			this.lock.writeLock().unlock();
		}

		/**
		 * null until run() completed
		 */
		long[] getCounters() {
			return this.counters;
		}
	}
}
