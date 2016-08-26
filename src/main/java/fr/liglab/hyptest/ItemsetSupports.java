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

import java.util.Arrays;

public class ItemsetSupports {
	private int[] itemset;
	private int support;

	public ItemsetSupports(int[] itemset, int s1) {
		super();
		this.itemset = itemset;
		this.support = s1;
	}

	public final int[] getItemset() {
		return itemset;
	}

	/**
	 * @return the support count set at pattern insertion
	 */
	public final int getSupport() {
		return support;
	}
	
	@Override
	public String toString() {
		return "ItemsetSupport [itemset=" + Arrays.toString(itemset) + ", support=" + support + "]";
	}
}
