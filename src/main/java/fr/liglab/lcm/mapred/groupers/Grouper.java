package fr.liglab.lcm.mapred.groupers;

import fr.liglab.lcm.mapred.Driver;
import gnu.trove.TIntCollection;

import org.apache.hadoop.conf.Configuration;

public abstract class Grouper {
	public abstract int getGid(int item);
	
	/**
	 * provided collection will be filled with group's items (in ascending order)
	 * 
	 * greatestItemID is a necessary computation hint 
	 */
	public abstract void fillWithGroupItems(TIntCollection collection, int gid, int greatestIID);
	
	public static Grouper factory(Configuration conf) {
		Grouper built = null;
		
		int nbGroups = conf.getInt(Driver.KEY_NBGROUPS, 50);
		String grouperName = conf.get(Driver.KEY_GROUPER_CLASS, "RoundRobin");
		
		if ("Slicer".equals(grouperName)) {
			int maxItemID = conf.getInt(Driver.KEY_REBASING_MAX_ID, 1);
			built = new Slicer(nbGroups, maxItemID);
		} else {
			built = new RoundRobin(nbGroups);
		}
		
		int singleGroupId = conf.getInt(Driver.KEY_SINGLE_GROUP_ID, -1);
		if (singleGroupId >= 0) {
			built = new SingleGroupWrapper(built, singleGroupId);
		}
		
		return built;
	}
}
