package fr.liglab.mining.util;

import java.util.Calendar;

import org.apache.hadoop.mapreduce.Reducer.Context;

import fr.liglab.mining.internals.FrequentsIterator;

/**
 * This thread will give some information about the progression on stderr every 5 minutes. 
 * When running on Hadoop it may also be used to poke the master node every 5 minutes so it 
 * doesn't kill the task.
 * 
 * you MUST use setInitState before starting the thread 
 * you MAY use setHadoopContext
 */
public class ProgressWatcherThread extends Thread {
	/**
	 * ping delay, in milliseconds
	 */
	private static final long PRINT_STATUS_EVERY = 5 * 60 * 1000;

	private FrequentsIterator startersIt;
	
	@SuppressWarnings("rawtypes")
	private Context hadoopContext = null;

	public void setStartersIterator(FrequentsIterator starters) {
		this.startersIt = starters;
	}

	@Override
	public void run() {
		while (true) {
			try {
				Thread.sleep(PRINT_STATUS_EVERY);
				System.err.format("%1$tY/%1$tm/%1$td %1$tk:%1$tM:%1$tS - root iterator state : %2$d/%3$d\n",
						Calendar.getInstance(), this.startersIt.peek(), this.startersIt.last());
				
				if (this.hadoopContext != null) {
					this.hadoopContext.progress();
				}
				
			} catch (InterruptedException e) {
				return;
			}
		}
	}

	@SuppressWarnings("rawtypes")
	public void setHadoopContext(Context context) {
		this.hadoopContext = context;
	}
}
