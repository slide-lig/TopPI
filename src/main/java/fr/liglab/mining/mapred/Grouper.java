package fr.liglab.mining.mapred;

import fr.liglab.mining.internals.FrequentsIterator;

public final class Grouper {
	
	public final int nbGroups;
	public final int maxItemID;
	
	public Grouper(int groupsCount, int maxItem) {
		this.nbGroups = groupsCount;
		this.maxItemID = maxItem;
	}
	
	public int getGroupId(int itemId) {
		return itemId % this.nbGroups;
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
}
