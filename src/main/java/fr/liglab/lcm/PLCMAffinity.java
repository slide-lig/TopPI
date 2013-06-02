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
		int corePerSocket = nbThreads / totalSockets; //AffinityLock.cpuLayout().coresPerSocket();
		int usedSockets = totalSockets; //nbThreads / corePerSocket;
		if (nbThreads % totalSockets != 0) {
			corePerSocket++;
		}
		if (usedSockets > totalSockets) {
			throw new IllegalArgumentException("nbThreads " + nbThreads + " seems to be too much for " + totalSockets
					+ " sockets, requires " + usedSockets);
		}
		//System.out.println("CPU layout\n" + AffinityLock.cpuLayout());
		System.err.println("using " + usedSockets + " socket(s) with " + corePerSocket + " threads per socket");
		int remainingThreads = nbThreads;
		this.threadsBySocket = new ArrayList<List<PLCMAffinityThread>>(usedSockets);
		int id = 0;
		AffinityLock[] socketLocks = new AffinityLock[usedSockets];
		socketLocks[0] = al.acquireLock(AffinityStrategies.SAME_SOCKET);
		for (int s = 1; s < usedSockets; s++) {
			//System.out.println("getting lock different socket as " + al.cpuId());
			socketLocks[s] = al.acquireLock(AffinityStrategies.DIFFERENT_SOCKET);
		}
		al.release();
		for (int s = 0; s < usedSockets; s++) {
			//System.out.println("positioning threads for socket " + s);
			
			List<PLCMAffinityThread> l = new ArrayList<PLCMAffinityThread>(corePerSocket);
			this.threadsBySocket.add(l);
			Semaphore bindingSem = new Semaphore(1);
			
			for (int c = 0; c < corePerSocket && remainingThreads > 0; c++, remainingThreads--, id++) {
				if (c == 0) {
					l.add(new PLCMAffinityThread(id, s, socketLocks[s], bindingSem, false));
				} else {
					//System.out.println("getting lock same socket as " + socketLocks[s].cpuId());
					l.add(new PLCMAffinityThread(id, s, socketLocks[s], bindingSem, true));
				}
			}
		}

		for (List<PLCMAffinityThread> l : threadsBySocket) {
			this.threads.addAll(l);
		}
	}

	@Override
	void initializeAndStartThreads(final ExplorationStep initState) {
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
				ExplorationStep copiedState = l.get(0).datasetToCopy;
				for (PLCMAffinityThread t : l) {
					t.init(copiedState);
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
		System.err.println("\nThe assignment of CPUs is\n" + AffinityLock.dumpLocks());
	}

	@Override
	public ExplorationStep stealJob(final PLCMThread t) {
		PLCMAffinityThread thief = (PLCMAffinityThread) t;
		// System.out.println(t.getName() + " trying to steal");
		// here we need to readlock because the owner thread can write
		for (int level = 0; level < thief.position.length; level++) {
			// System.out.println("level " + level + " candidates " +
			// this.threads.size());
			for (PLCMThread v : this.threads) {
				PLCMAffinityThread victim = (PLCMAffinityThread) v;
				if (victim == thief) {
					continue;
				}
				
				/*
				 *  FIXME why not simply steal
				 *  1. from fellow threads in threadsBySocket
				 *  2. otherwise, from any other in this.threads
				 *  ?
				 *  
				 *  actually, it may be what's done here...
				 */
				
				
				boolean steal = true;
				for (int higherLevel = level + 1; higherLevel < thief.position.length; higherLevel++) {
					if (thief.position[higherLevel] != victim.position[higherLevel]) {
						// System.out.println(thief.getName() +
						// " cannot steal from " + victim.getName() +
						// " at level "
						// + level + " because of higher level " + higherLevel);
						steal = false;
						break;
					}
				}
				if (steal) {
					// System.out.println(thief.getName() +
					// " tries to steal from " + victim.getName() + " at level "
					// + level);
					ExplorationStep e = stealJob(thief, victim);
					if (e != null) {
						// System.out.println(thief.getName() +
						// " great success stealing from " + victim.getName()
						// + " at level " + level);
						return e;
					}
				}
			}
		}
		//System.out.println(thief.getName() + " didn't manage to steal anything, bad thief");
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
		private AffinityLock al;
		private final int group;
		private Semaphore datasetCopySem;
		private Semaphore initializedSem;
		private ExplorationStep datasetToCopy;
		private Semaphore socketMatesSem;
		private final boolean getNewAL;
		
		private PLCMAffinityThread(int id, int group, AffinityLock al, Semaphore bindingSem, boolean getNewAL) {
			super(id);
			this.al = al;
			this.position = new int[3];
			this.group = group;
			this.socketMatesSem = bindingSem;
			this.getNewAL = getNewAL;
		}

		private void prepareForCopy(ExplorationStep datasetToCopy, Semaphore datasetCopySem, Semaphore initializedSem) {
			this.datasetToCopy = datasetToCopy;
			this.datasetCopySem = datasetCopySem;
			this.initializedSem = initializedSem;
		}

		@Override
		public void run() {
			/// binding one thread at a time
			
			try {
				this.socketMatesSem.acquire();
			} catch (InterruptedException e) {
				e.printStackTrace();
				System.exit(-1);
			}
			
			if (this.getNewAL) {
				this.al = this.al.acquireLock(AffinityStrategies.SAME_SOCKET);
			}
			this.al.bind(false); // WARN: using bind(true) leads to strange libaffinity behaviors
			
			this.socketMatesSem.release();
			this.socketMatesSem = null;
			
			this.position[0] = al.cpuId();
			this.position[1] = AffinityLock.cpuLayout().coreId(al.cpuId());
			this.position[2] = AffinityLock.cpuLayout().socketId(al.cpuId());
			this.setName("Thread #" + this.id + " group " + this.group + " placed on " + Arrays.toString(this.position));
			
			/// maybe copy the initial dataset
			
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
			
			super.run();
			al.release();
		}
	}
}
