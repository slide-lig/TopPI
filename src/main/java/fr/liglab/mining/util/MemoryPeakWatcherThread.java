package fr.liglab.mining.util;

public class MemoryPeakWatcherThread extends Thread {
	/**
	 * garbage-collect-n-peek delay, in milliseconds
	 */
	private static final long GC_AND_CHECK_DELAY = 15000;
	
	/**
	 * In bytes
	 */
	private long maxUsedMemory = 0; 
	
	private Runtime runtime;
	
	public long getMaxUsedMemory() {
		return maxUsedMemory;
	}
	
	@Override
	public void run() {
		this.runtime = Runtime.getRuntime();
		this.maxUsedMemory = 0;
		this.peek();
		
		while (true) {
			try {
				Thread.sleep(GC_AND_CHECK_DELAY);
				this.peek();
			} catch (InterruptedException e) {
				this.peek();
				return;
			}
		}
	}
	
	private void peek() {
		this.runtime.gc();
		
		final long used = this.runtime.totalMemory() - this.runtime.freeMemory();
		
		if (used > this.maxUsedMemory) {
			this.maxUsedMemory = used;
		}
	}
}
