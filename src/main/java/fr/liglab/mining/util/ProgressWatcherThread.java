package fr.liglab.mining.util;

import java.util.Calendar;

import fr.liglab.mining.internals.ExplorationStep;
import fr.liglab.mining.internals.ExplorationStep.Progress;

public class ProgressWatcherThread extends Thread {
	/**
	 * ping delay, in milliseconds
	 */
	private static final long PRINT_STATUS_EVERY = 5 * 60 * 1000;

	private final ExplorationStep step;

	public ProgressWatcherThread(ExplorationStep initState) {
		this.step = initState;
	}

	@Override
	public void run() {
		while (true) {
			try {
				Thread.sleep(PRINT_STATUS_EVERY);
				Progress progress = this.step.getProgression();
				System.err.format("%1$tY/%1$tm/%1$td %1$tk:%1$tM:%1$tS - root iterator state : %2$d/%3$d\n",
						Calendar.getInstance(), progress.current, progress.last);
			} catch (InterruptedException e) {
				return;
			}
		}
	}
}
