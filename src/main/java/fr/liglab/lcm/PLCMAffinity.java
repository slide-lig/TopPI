package fr.liglab.lcm;

import java.util.Arrays;
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

	private PLCMAffinityThread[][] threadsBySocket;
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
		int usedSockets = totalSockets;//nbThreads / corePerSocket;
		int corePerSocket = nbThreads / usedSockets;
		if (nbThreads % usedSockets != 0) {
			corePerSocket++;
		}
		if (usedSockets > totalSockets) {
			throw new IllegalArgumentException("nbThreads " + nbThreads + " seems to be too much for " + totalSockets
					+ " sockets, requires " + usedSockets);
		}
		//System.out.println("CPU layout\n" + AffinityLock.cpuLayout());
		System.err.println("using " + usedSockets + " socket(s) with " + corePerSocket + " threads per socket");
		
		AffinityLock[] socketLocks = new AffinityLock[usedSockets];
		socketLocks[0] = al.acquireLock(AffinityStrategies.SAME_SOCKET);
		for (int s = 1; s < usedSockets; s++) {
			socketLocks[s] = al.acquireLock(AffinityStrategies.DIFFERENT_SOCKET);
		}
		al.release();

		int id = 0;
		int remainingThreads = nbThreads;
		this.threadsBySocket = new PLCMAffinityThread[usedSockets][];
		
		for (int s = 0; s < usedSockets; s++) {
			PLCMAffinityThread[] onThisSocket = new PLCMAffinityThread[Math.min(corePerSocket, remainingThreads)];
			this.threadsBySocket[s] = onThisSocket;
			Semaphore bindingSem = new Semaphore(1);
			
			for (int c = 0; c < corePerSocket && remainingThreads > 0; c++, id++, remainingThreads--) {
				PLCMAffinityThread t = new PLCMAffinityThread(id, s, socketLocks[s], bindingSem, (c != 0));
				onThisSocket[c] = t;
				this.threads.add(t);
			}
		}
	}

	@Override
	void initializeAndStartThreads(final ExplorationStep initState) {
		
		Semaphore copySem = new Semaphore(0);
		int nbCopies = 0;
		for (int socketId = 0; socketId < this.threadsBySocket.length; socketId++) {
			if (socketId % this.copySocketModulo == 0) {
				if (socketId == 0) {
					// we're on the same socket as the main thread, which created initState
					PLCMAffinityThread t = this.threadsBySocket[socketId][0];
					t.init(initState);
					t.start();
				} else {
					nbCopies++;
					PLCMAffinityThread t = this.threadsBySocket[socketId][0];
					t.prepareForCopy(initState, copySem);
					t.start();
				}
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
		
		//// copies done. now it's time to start follower threads, which will simply start by stealing
		for (PLCMThread thread : this.threads) {
			if (!thread.isAlive()) {
				thread.start();
			}
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
		
		/// FIXME this will only work if copySocketModulo is ==1 or ==usedSockets
		
		for (PLCMAffinityThread fellow : this.threadsBySocket[thief.socketId]) {
			if (fellow == thief) {
				continue;
			}
			
			ExplorationStep e = stealJob(thief, fellow);
			if (e != null) {
				return e;
			}
		}
		
		for (int socketId = 0; socketId < this.threadsBySocket.length; socketId++) {
			if (socketId != thief.socketId) {
				for (PLCMAffinityThread victim : this.threadsBySocket[socketId]) {
					ExplorationStep e = stealJob(thief, victim);
					if (e != null) {
						return e;
					}
				}
			}
		}
		
		System.out.println(thief.getName() + " didn't manage to steal anything, bad thief");
		
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
		private final int socketId;
		private Semaphore datasetCopySem = null;
		private ExplorationStep datasetToCopy = null;
		private Semaphore socketMatesSem;
		private final boolean getNewAL;
		
		private PLCMAffinityThread(int id, int socket, AffinityLock al, Semaphore bindingSem, boolean getNewAL) {
			super(id);
			this.al = al;
			this.position = new int[3];
			this.socketId = socket;
			this.socketMatesSem = bindingSem;
			this.getNewAL = getNewAL;
		}

		private void prepareForCopy(ExplorationStep datasetToCopy, Semaphore datasetCopySem) {
			this.datasetToCopy = datasetToCopy;
			this.datasetCopySem = datasetCopySem;
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
			this.setName("Thread #" + this.id + " placed on " + Arrays.toString(this.position));
			
			/// maybe copy the initial dataset
			
			if (this.datasetCopySem != null) {
				this.init(datasetToCopy.copy());
				datasetCopySem.release();
				datasetCopySem = null;
			}
			
			super.run();
			al.release();
		}
	}
}
