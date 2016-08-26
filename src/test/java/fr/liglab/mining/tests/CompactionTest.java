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
package fr.liglab.mining.tests;

import org.junit.Test;

import fr.liglab.mining.internals.Counters;
import fr.liglab.mining.internals.Dataset.TransactionsIterable;
import fr.liglab.mining.internals.DenseCounters;
import fr.liglab.mining.internals.ExplorationStep;

public class CompactionTest {

	@Test
	public void test() {
		
		ExplorationStep init = new ExplorationStep(2, FileReaderTest.PATH_MICRO, 10);
		
		TransactionsIterable support = init.dataset.getSupport(1);
		Counters candidateCounts = new DenseCounters(2, support.iterator(), 
				1, null, 5, init.counters.getReverseRenaming(), new int[] {});
		int[] renaming = candidateCounts.compressRenaming(init.counters.getReverseRenaming());
		
	}

}
