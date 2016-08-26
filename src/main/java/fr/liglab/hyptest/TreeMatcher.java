/*
	This file is part of TopPI - see https://github.com/slide-lig/TopPI/
	
	Copyright 2016 Martin Kirchgessner, Vincent Leroy, Alexandre Termier, Sihem Amer-Yahia, Marie-Christine Rousset, Université Grenoble Alpes, LIG, CNRS
	Licensed under the Apache License, Version 2.0 (the "License");
	you may not use this file except in compliance with the License.
	You may obtain a copy of the License at
	 http://www.apache.org/licenses/LICENSE-2.0
	 
	or see the LICENSE.txt file joined with this program.
	Unless required by applicable law or agreed to in writing, software
	distributed under the License is distributed on an "AS IS" BASIS,
	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
	See the License for the specific language governing permissions and
	limitations under the License.
*/
package fr.liglab.hyptest;

import gnu.trove.list.TIntList;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntObjectHashMap;

import java.util.Iterator;
/*
 * Assumes all transactions and patterns are sorted in asc order
 * @author vleroy
 */
public class TreeMatcher implements Iterable<ItemsetSupports>{
	
	private TreeNode root;

	public TreeMatcher() {
		this.root = new TreeNode(-1, null, false);
	}

	public void match(int[] transaction) {
		this.match(transaction, this.root, 0);
	}

	public void match(int[] transaction, TreeNode n, int start) {
		if (n.isMonitored()) {
			n.addMatch();
		}
		for (int i = start; i < transaction.length; i++) {
			TreeNode iBranch = n.getLink(transaction[i]);
			if (iBranch != null) {
				this.match(transaction, iBranch, i + 1);
			}
		}
	}

	public void match(int[] buffer, int length) {
		this.match(buffer, length, this.root, 0);
	}

	public void match(int[] buffer, int length, TreeNode n, int start) {
		if (n.isMonitored()) {
			n.addMatch();
		}
		for (int i = start; i < length; i++) {
			TreeNode iBranch = n.getLink(buffer[i]);
			if (iBranch != null) {
				this.match(buffer, length, iBranch, i + 1);
			}
		}
	}
	
	public TreeNode addPattern(int[] pattern) {
		return this.addPattern(pattern, -1, Integer.MIN_VALUE);
	}
	
	public TreeNode addPattern(int[] pattern, int support, int without) {
		TreeNode pos = root;
		for (int i = 0; i < pattern.length; i++) {
			final int item = pattern[i];
			if (item != without) {
				TreeNode next = pos.getLink(item);
				if (next == null) {
					next = new TreeNode(item, pos, false);
					pos.addLink(item, next);
				}
				pos = next;
			}
		}
		pos.setMonitored(true);
		if (support > 0) {
			pos.setNbMatch(support);
		}
		return pos;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		for (ItemsetSupports its : this) {
			sb.append(its);
			sb.append("\n");
		}
		return sb.toString();
	}

	private static class TreeNode {
		private final static int[] emptyArray = {};
		private TIntObjectMap<TreeNode> links;
		private boolean isMonitored;
		private int nbMatch = 0;
		private final TreeNode parent;
		private final int item;

		private TreeNode(int item, TreeNode parent, boolean isMonitored) {
			this.isMonitored = isMonitored;
			this.parent = parent;
			this.item = item;
		}
		
		private TreeNode getLink(int l) {
			if (this.links == null) {
				return null;
			} else {
				return this.links.get(l);
			}
		}

		private void addLink(int l, TreeNode n) {
			if (this.links == null) {
				this.links = new TIntObjectHashMap<TreeNode>();
			}
			this.links.put(l, n);
		}

		private int[] getLinks() {
			if (this.links == null) {
				return emptyArray;
			} else {
				return this.links.keys();
			}
		}

		private boolean isMonitored() {
			return this.isMonitored;
		}

		private void setMonitored(boolean m) {
			this.isMonitored = m;
		}

		private void addMatch() {
			this.nbMatch++;
		}
		
		private void setNbMatch(int i) {
			this.nbMatch = i;
		}

		private final int getNbMatch() {
			return nbMatch;
		}
		
		@Override
		public String toString() {
			StringBuilder sb = new StringBuilder();
			if (this.parent == null) {
				return "root";
			}
			TreeNode n = this;
			while (n.parent != null) {
				sb.append(" " + n.item);
				n = n.parent;
			}
			sb.reverse();
			if (this.isMonitored) {
				sb.append("match=" + this.nbMatch);
			}
			return sb.toString();
		}

		private int getItem() {
			return this.item;
		}

		private TreeNode getParent() {
			return this.parent;
		}

		private boolean hasChildren() {
			return this.links != null && !this.links.isEmpty();
		}

		private void removeLink(int item) {
			this.links.remove(item);
		}
	}
	
	
	@Override
	public Iterator<ItemsetSupports> iterator() {
		return new Iterator<ItemsetSupports>() {

			private TreeNode currentNode = root;
			private TreeNode lastReturned = null;
			private TIntList itemset = new TIntArrayList();
			private int previousChild = -1;
			{
				if (!currentNode.hasChildren()) {
					currentNode = null;
				} else {
					this.moveToNext();
				}
			}

			@Override
			public void remove() {
				// FIXME - test with TreeMatcher.main
				this.lastReturned.setMonitored(false);
				if (!this.lastReturned.hasChildren()) {
					TreeNode tn = this.lastReturned;
					this.lastReturned = null;
					while (!tn.isMonitored() && tn.getParent() != null) {
						TreeNode parent = tn.getParent();
						parent.removeLink(tn.getItem());
						tn = parent;
					}
				}
			}

			@Override
			public ItemsetSupports next() {
				this.lastReturned = this.currentNode;
				ItemsetSupports res = new ItemsetSupports(itemset.toArray(), currentNode.getNbMatch());
				this.moveToNext();
				return res;
			}

			private void moveToNext() {
				if (this.previousChild != -1) {
					// means we backtracked to here
					int previousChildPos = -1;
					for (int i = 0; i < this.currentNode.getLinks().length; i++) {
						if (this.currentNode.getLinks()[i] == this.previousChild) {
							previousChildPos = i;
							break;
						}
					}
					if (previousChildPos == this.currentNode.getLinks().length - 1) {
						if (this.currentNode.getParent() == null) {
							this.currentNode = null;
							return;
						} else {
							// backtrack
							this.previousChild = this.currentNode.getItem();
							this.currentNode = this.currentNode.getParent();
							this.itemset.removeAt(this.itemset.size() - 1);
							this.moveToNext();
						}
					} else {
						// go to next child
						this.previousChild = -1;
						this.currentNode = this.currentNode.getLink(this.currentNode.getLinks()[previousChildPos + 1]);
						itemset.add(currentNode.getItem());
						while (!currentNode.isMonitored()) {
							currentNode = currentNode.getLink(currentNode.getLinks()[0]);
							itemset.add(currentNode.getItem());
						}
					}
				} else {
					// means we came here going deeper
					if (this.currentNode.hasChildren()) {
						// go deeper
						do {
							currentNode = currentNode.getLink(currentNode.getLinks()[0]);
							itemset.add(currentNode.getItem());
						} while (!currentNode.isMonitored());
					} else {
						// backtrack
						this.previousChild = this.currentNode.getItem();
						this.currentNode = this.currentNode.getParent();
						this.itemset.removeAt(this.itemset.size() - 1);
						this.moveToNext();
					}
				}
			}

			@Override
			public boolean hasNext() {
				return this.currentNode != null;
			}
		};
	}

	public int getSupport(int[] itemset) throws NotMonitoredException {
		TreeNode n = this.root;
		for (int i = 0; i < itemset.length; i++) {
			n = n.getLink(itemset[i]);
			if (n == null) {
				throw new NotMonitoredException();
			} else if (i == itemset.length - 1) {
				if (!n.isMonitored()) {
					throw new NotMonitoredException();
				} else {
					return n.getNbMatch();
				}
			}
		}
		throw new NotMonitoredException();
	}

	public static class NotMonitoredException extends Exception {
		private static final long serialVersionUID = 1L;
	}

	public static void main(String[] args) {
		TreeMatcher tm = new TreeMatcher();
		tm.addPattern(new int[] { 1, 2, 3, 4 });
		tm.addPattern(new int[] { 1, 2, 3, 5 });
		tm.addPattern(new int[] { 1, 2 });
		tm.addPattern(new int[] { 1, 3 });
		tm.addPattern(new int[] { 2, 7, 8 }, 123, 2);
		tm.addPattern(new int[] { 2, 7, 8 }, 122, 7);
		tm.addPattern(new int[] { 2, 7, 8 }, 121, 8);
		tm.addPattern(new int[] { 3 });
		System.out.println(tm.toString());
		tm.match(new int[] { 1 });
		tm.match(new int[] { 1, 2, 3, 4, 5, 6, 7, 8 });
		tm.match(new int[] { 1, 4 });
		tm.match(new int[] { 1, 2, 3 });
		tm.match(new int[] { 1, 5 });
		System.out.println(tm.toString());
		Iterator<ItemsetSupports> it = tm.iterator();
		for (int i = 0; i < 3; i++) {
			it.next();
		}
		it.remove();
		System.out.println(tm.toString());
	}
}
