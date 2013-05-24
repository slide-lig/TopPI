package fr.liglab.lcm;

import java.util.ArrayList;
import java.util.List;

import com.higherfrequencytrading.affinity.AffinityLock;
import com.higherfrequencytrading.affinity.AffinityStrategies;

import fr.liglab.lcm.internals.nomaps.ExplorationStep;
import fr.liglab.lcm.io.PatternsCollector;
import gnu.trove.list.TIntList;
import gnu.trove.list.array.TIntArrayList;

/**
 * LCM implementation, based on UnoAUA04 :
 * "An Efficient Algorithm for Enumerating Closed Patterns in Transaction Databases"
 * by Takeaki Uno el. al.
 */
public class PLCMAffinity extends PLCM {
	private List<List<PLCMAffinityThread>> threadsBySocket;

	public PLCMAffinity(PatternsCollector patternsCollector) {
		super(patternsCollector);
	}

	private PLCMAffinity(PatternsCollector patternsCollector, int nbThreads) {
		super(patternsCollector, nbThreads);
	}

	@Override
	void createThreads(int nbThreads) {
		int totalSockets = AffinityLock.cpuLayout().sockets();
		int corePerSocket = AffinityLock.cpuLayout().coresPerSocket();
		int usedSockets = nbThreads / corePerSocket;
		if (nbThreads % corePerSocket != 0) {
			usedSockets++;
		}
		if (usedSockets > totalSockets) {
			throw new IllegalArgumentException("nbThreads " + nbThreads + " seems to be too much for " + totalSockets
					+ " sockets, requires " + usedSockets);
		}
		System.out.println("CPU layout\n" + AffinityLock.cpuLayout());
		System.out.println("using " + usedSockets + " socket(s) with " + corePerSocket + " threads per socket");
		int remainingThreads = nbThreads;
		this.threadsBySocket = new ArrayList<List<PLCMAffinityThread>>(usedSockets);
		AffinityLock al = AffinityLock.acquireLock();
		int id = 0;
		for (int s = 0; s < usedSockets; s++) {
			System.out.println("creating threads for socket " + s);
			List<PLCMAffinityThread> l = new ArrayList<PLCMAffinityThread>(corePerSocket);
			this.threadsBySocket.add(l);
			AffinityLock socketLock;
			if (s == 0) {
				socketLock = al.acquireLock(AffinityStrategies.SAME_CORE);
			} else {
				socketLock = al.acquireLock(AffinityStrategies.DIFFERENT_CORE);
			}
			for (int c = 0; c < corePerSocket && remainingThreads > 0; c++, remainingThreads--) {
				if (c == 0) {
					l.add(new PLCMAffinityThread(id, socketLock));
					id++;
				} else {
					l.add(new PLCMAffinityThread(id, socketLock.acquireLock(AffinityStrategies.SAME_SOCKET)));
					id++;
				}
			}
		}
		al.release();
		for (List<PLCMAffinityThread> l : threadsBySocket) {
			this.threads.addAll(l);
		}
		System.out.println("\nThe assignment of CPUs is\n" + AffinityLock.dumpLocks());
		System.exit(-1);
	}

	@Override
	void initializedThreads(ExplorationStep initState) {
		for (int i = 0; i < this.threadsBySocket.size(); i++) {
			List<PLCMAffinityThread> l = this.threadsBySocket.get(i);
			if (i != 0) {
				initState = initState.copy();
			}
			for (PLCMAffinityThread t : l) {
				t.init(initState);
			}
		}
	}

	@Override
	public ExplorationStep stealJob(final PLCMThread t) {
		PLCMAffinityThread thief = (PLCMAffinityThread) t;
		// here we need to readlock because the owner thread can write
		for (int level = 0; level < thief.position.size(); level++) {
			for (PLCMThread v : this.threads) {
				PLCMAffinityThread victim = (PLCMAffinityThread) v;
				if (victim == thief) {
					break;
				}
				for (int higherLevel = level + 1; higherLevel < thief.position.size(); higherLevel++) {
					if (thief.position.get(higherLevel) != victim.position.get(higherLevel)) {
						break;
					}
				}
				for (int stealPos = 0; stealPos < victim.stackedJobs.size(); stealPos++) {
					victim.lock.readLock().lock();
					if (!victim.stackedJobs.isEmpty()) {
						ExplorationStep sj = victim.stackedJobs.get(0);
						victim.lock.readLock().unlock();

						ExplorationStep next = sj.next();

						if (next != null) {
							return next;
						}
					} else {
						victim.lock.readLock().unlock();
					}
				}
			}
		}
		return null;
	}

	/**
	 * Some classes in EnumerationStep may declare counters here. see references
	 * to PLCMThread.counters
	 */
	public enum PLCMCounters {
		ExplorationStepInstances, ExplorationStepCatchedWrongFirstParents, FirstParentTestRejections, TopKRejections,
	}

	public class PLCMAffinityThread extends PLCMThread {
		private final TIntList position;
		private final AffinityLock al;

		private PLCMAffinityThread(int id, AffinityLock al) {
			super(id);
			this.al = al;
			this.position = new TIntArrayList(3);
		}

		private void init(ExplorationStep initState) {
			this.lock.writeLock().lock();
			this.stackedJobs.add(initState);
			this.lock.writeLock().unlock();
		}

		@Override
		public void run() {
			al.bind(true);
			this.position.add(al.cpuId());
			this.position.add(AffinityLock.cpuLayout().coreId(al.cpuId()));
			this.position.add(AffinityLock.cpuLayout().socketId(al.cpuId()));
			this.setName("PLCMThread " + position);
			this.setName(this.getName() + " at " + this.position);
			super.run();
			al.release();
		}
	}
}
