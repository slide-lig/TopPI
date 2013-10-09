package fr.liglab.mining.mapred;

import fr.liglab.mining.TopLCM.TopLCMCounters;
import fr.liglab.mining.internals.ExplorationStep;
import fr.liglab.mining.internals.FrequentsIterator;
import fr.liglab.mining.internals.Selector;

public class Grouper {
	
	public final int nbGroups;
	public final int maxItemID;
	
	public Grouper(int groupsCount, int maxItem) {
		this.nbGroups = groupsCount;
		this.maxItemID = maxItem;
	}
	
	/**
	 * @param itemId
	 * @return item's groupID or -1 if we should dump the corresponding group (see SingleGroup)
	 */
	public int getGroupId(int itemId) {
		return itemId % this.nbGroups;
	}
	
	public static final class SingleGroup extends Grouper {
		private final int groupId;
		
		public SingleGroup(int groupsCount, int maxItem, int singleGroupID) {
			super(groupsCount, maxItem);
			this.groupId = singleGroupID;
		}
		
		@Override
		public int getGroupId(int itemId) {
			return (super.getGroupId(itemId) == this.groupId) ? this.groupId : -1;
		}
	}
	
	public FrequentsIterator getGroupItems(int groupId) {
		return new ItemsInGroupIterator(groupId, this.maxItemID);
	}
	
	private final class ItemsInGroupIterator implements FrequentsIterator {
		
		private int current;
		private final int max;
		
		public ItemsInGroupIterator(int start, int last) {
			this.current = start - nbGroups;
			this.max = last;
		}
		
		@Override
		public int next() {
			if (this.current == Integer.MIN_VALUE) {
				return -1;
			} else {
				this.current += nbGroups;
				
				if (this.current > this.max) {
					this.current = Integer.MIN_VALUE;
					return -1;
				} else {
					return this.current;
				}
			}
		}

		@Override
		public int peek() {
			return this.current;
		}

		@Override
		public int last() {
			return this.max;
		}
	}
	
	public Selector getStartersSelector(Selector next, int groupId) {
		return new StartersSelector(next, groupId);
	}
	
	/**
	 * This selector does not copy itself !!
	 * This means that it must be the selector chain's head, use it to restrict the exploration 
	 * of an initial ExplorationState to group's items
	 */
	private final class StartersSelector extends Selector {
		
		private final int gid;
		
		public StartersSelector(Selector follower, int groupId) {
			super(follower);
			this.gid = groupId;
		}
		
		@Override
		protected boolean allowExploration(int extension, ExplorationStep state)
				throws WrongFirstParentException {
			
			return (extension % nbGroups) == this.gid;
		}

		@Override
		protected Selector copy(Selector newNext) {
			return newNext;
		}

		@Override
		protected TopLCMCounters getCountersKey() {
			return null;
		}
	}
}
