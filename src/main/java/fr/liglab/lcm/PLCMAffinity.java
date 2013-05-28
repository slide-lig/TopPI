package fr.liglab.lcm;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Semaphore;

import com.higherfrequencytrading.affinity.AffinityLock;
import com.higherfrequencytrading.affinity.AffinityStrategies;

import fr.liglab.lcm.internals.ExplorationStep;
import fr.liglab.lcm.io.PatternsCollector;

/**
 * LCM implementation, based on UnoAUA04 :
 * "An Efficient Algorithm for Enumerating Closed Patterns in Transaction Databases"
 * by Takeaki Uno el. al.
 */
public class PLCMAffinity extends PLCM {
	private static AffinityLock al;

	static void bindMainThread() {
		al = AffinityLock.acquireLock(false); // lock a core for main thread
		al.bind(false); // bind main thread to current (locked) core - false=> allow another thread if HT
	}

	private List<List<PLCMAffinityThread>> threadsBySocket;
	private final int copySocketModulo;

	public PLCMAffinity(PatternsCollector patternsCollector) {
		this(patternsCollector, Runtime.getRuntime().availableProcessors());
	}

	public PLCMAffinity(PatternsCollector patternsCollector, int nbThreads) {
		this(patternsCollector, nbThreads, 1);
	}

	public PLCMAffinity(PatternsCollector patternsCollector, int nbSocketsShareCopy, boolean b) {
		this(patternsCollector, Runtime.getRuntime().availableProcessors(), nbSocketsShareCopy);
	}

	public PLCMAffinity(PatternsCollector patternsCollector, int nbThreads, int copySocketModulo) {
		super(patternsCollector, nbThreads);
		this.copySocketModulo = copySocketModulo;
		if (copySocketModulo < 1 || copySocketModulo > AffinityLock.cpuLayout().sockets()) {
			al.release();
			throw new IllegalArgumentException("copySocketModulo has to be > 0 and <= number of sockets, given "
					+ nbThreads);
		}
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
		int id = 0;
		AffinityLock[] socketLocks = new AffinityLock[usedSockets];
		for (int s = 0; s < usedSockets; s++) {
			if (s == 0) {
				System.out.println("getting lock same socket as " + al.cpuId());
				socketLocks[s] = al.acquireLock(AffinityStrategies.SAME_SOCKET);
			} else {
				System.out.println("getting lock different socket as " + al.cpuId());
				socketLocks[s] = al.acquireLock(AffinityStrategies.DIFFERENT_SOCKET);
			}
		}
		al.release();
		for (int s = 0; s < usedSockets; s++) {
			System.out.println("positioning threads for group " + s);
			List<PLCMAffinityThread> l = new ArrayList<PLCMAffinityThread>(corePerSocket);
			this.threadsBySocket.add(l);
			for (int c = 0; c < corePerSocket && remainingThreads > 0; c++, remainingThreads--) {
				if (c == 0) {
					l.add(new PLCMAffinityThread(id, s, socketLocks[s]));
					id++;
				} else {
					System.out.println("getting lock same socket as " + socketLocks[s].cpuId());
					l.add(new PLCMAffinityThread(id, s, socketLocks[s].acquireLock(AffinityStrategies.SAME_SOCKET)));
					id++;
				}
			}
		}

		for (List<PLCMAffinityThread> l : threadsBySocket) {
			this.threads.addAll(l);
		}
	}

	@Override
	void initializeAndStartThreads(ExplorationStep initState) {
		Semaphore copySem = new Semaphore(0);
		Semaphore initializedSem = new Semaphore(0);
		int nbCopies = 0;
		for (int i = 0; i < this.threadsBySocket.size(); i++) {
			List<PLCMAffinityThread> l = this.threadsBySocket.get(i);
			if (i != 0 && i % this.copySocketModulo == 0) {
				// first socket gets the initial dataset because it should be on
				// the same socket as the main thread which created it
				nbCopies++;
				PLCMAffinityThread t = l.get(0);
				t.prepareForCopy(initState, copySem, initializedSem);
				t.start();
			}
		}
		if (nbCopies > 0) {
			try {
				copySem.acquire(nbCopies);
			} catch (InterruptedException e) {
				e.printStackTrace();
				System.exit(-1);
			}
		}
		for (int i = 0; i < this.threadsBySocket.size(); i++) {
			List<PLCMAffinityThread> l = this.threadsBySocket.get(i);
			if (i == 0 || i % this.copySocketModulo != 0) {
				for (PLCMAffinityThread t : l) {
					t.init(initState);
				}
			} else {
				initState = l.get(0).datasetToCopy;
				for (PLCMAffinityThread t : l) {
					t.init(initState);
				}
			}
		}
		for (int i = 0; i < this.threadsBySocket.size(); i++) {
			List<PLCMAffinityThread> l = this.threadsBySocket.get(i);
			if (i == 0 || i % this.copySocketModulo != 0) {
				for (PLCMAffinityThread t : l) {
					t.start();
				}
			} else {
				for (int j = 1; j < l.size(); j++) {
					l.get(j).start();
				}
			}
		}
		if (nbCopies > 0) {
			initializedSem.release(nbCopies);
		}
		try {
			Thread.sleep(2000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		System.out.println("\nThe assignment of CPUs is\n" + AffinityLock.dumpLocks());
		System.exit(-1);
	}

	@Override
	public ExplorationStep stealJob(final PLCMThread t) {
		PLCMAffinityThread thief = (PLCMAffinityThread) t;
		// here we need to readlock because the owner thread can write
		for (int level = 0; level < thief.position.length; level++) {
			for (PLCMThread v : this.threads) {
				PLCMAffinityThread victim = (PLCMAffinityThread) v;
				if (victim == thief) {
					break;
				}
				boolean steal = true;
				for (int higherLevel = level + 1; higherLevel < thief.position.length; higherLevel++) {
					if (thief.position[higherLevel] != victim.position[higherLevel]) {
						steal = false;
						break;
					}
				}
				if (steal) {
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

	private class PLCMAffinityThread extends PLCMThread {
		private final int[] position;
		private final AffinityLock al;
		private final int group;
		private Semaphore datasetCopySem;
		private Semaphore initializedSem;
		private ExplorationStep datasetToCopy;

		private PLCMAffinityThread(int id, int group, AffinityLock al) {
			super(id);
			this.al = al;
			this.position = new int[3];
			this.group = group;
		}

		private void prepareForCopy(ExplorationStep datasetToCopy, Semaphore datasetCopySem, Semaphore initializedSem) {
			this.datasetToCopy = datasetToCopy;
			this.datasetCopySem = datasetCopySem;
			this.initializedSem = initializedSem;
		}

		@Override
		public void run() {
			al.bind(true);
			if (this.datasetCopySem != null) {
				datasetToCopy = datasetToCopy.copy();
				datasetCopySem.release();
				try {
					initializedSem.acquire();
				} catch (InterruptedException e) {
					e.printStackTrace();
					System.exit(-1);
				}
				datasetCopySem = null;
				initializedSem = null;
				datasetToCopy = null;

			}
			this.position[0] = al.cpuId();
			this.position[1] = AffinityLock.cpuLayout().coreId(al.cpuId());
			this.position[2] = AffinityLock.cpuLayout().socketId(al.cpuId());
			this.setName(this.getName() + " group " + this.group + " placed on " + Arrays.toString(this.position));
			System.out.println(this.getName());
			super.run();
			al.release();
		}
	}
}
