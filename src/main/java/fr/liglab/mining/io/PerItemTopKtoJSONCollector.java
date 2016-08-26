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
package fr.liglab.mining.io;

import java.util.Map;

import fr.liglab.mining.internals.ExplorationStep;

public class PerItemTopKtoJSONCollector extends PerItemTopKCollector {
	
	private Map<Integer, String> idMap;
	
	public PerItemTopKtoJSONCollector(final int k, final ExplorationStep initState, Map<Integer, String> itemIDmap){
		
		super(k, initState);
		
		this.idMap = itemIDmap;
	}
	
	@Override
	public long close() {
		long nbPatterns = 0;
		boolean interKeyComma = false;
		System.out.println("{");
		
		for (final int k : this.topK.keys()) {
			if (interKeyComma){
				System.out.println(",");
			} else {
				interKeyComma = true;
			}
			
			System.out.printf("\"%s\":[", getSafeName(k));
			
			final PatternWithFreq[] itemTopK = this.topK.get(k);
			for (int i = 0; i < itemTopK.length; i++) {
				if (itemTopK[i] == null) {
					break;
				} else {
					if (i>0) {
						System.out.print(",");
					}
					System.out.printf("{\"sup\":%d,\"set\":[", itemTopK[i].getSupportCount());
					int[] pattern = itemTopK[i].getPattern();
					for (int j = 0; j < pattern.length; j++) {
						if (j>0) {
							System.out.print(",");
						}
						System.out.printf("\"%s\"", getSafeName(pattern[j]));
					}
					System.out.print("]}");
					nbPatterns++;
				}
			}
			
			System.out.print("]");
		}
		
		
		System.out.println("\n}");
		return nbPatterns;
	}
	
	private String getSafeName(int id){
		return this.idMap.get(id).replace("\\", "\\\\").replace("\"", "\\\"");
	}
}
