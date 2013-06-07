package fr.liglab.lcm.util;

public class MemoryPeakWatcherThread extends Thread {
	/**
	 * garbage-collect-n-peek delay, in milliseconds
	 */
	private static final long GC_AND_CHECK_DELAY = 30000;
	
	/**
	 * In bytes
	 */
	private long maxUsedMemory = 0; 
	
	public long getMaxUsedMemory() {
		return maxUsedMemory;
	}
	
	@Override
	public void run() {
		Runtime runtime = Runtime.getRuntime();
		this.maxUsedMemory = runtime.totalMemory() - runtime.freeMemory();
		
		while (true) {
			try {
				Thread.sleep(GC_AND_CHECK_DELAY);
				this.peek(runtime);
			} catch (InterruptedException e) {
				this.peek(runtime);
				return;
			}
		}
	}
	
	private void peek(Runtime runtime) {
		runtime.gc();
		
		final long used = runtime.totalMemory() - runtime.freeMemory();
		
		if (used > this.maxUsedMemory) {
			this.maxUsedMemory = used;
		}
	}
}
