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
