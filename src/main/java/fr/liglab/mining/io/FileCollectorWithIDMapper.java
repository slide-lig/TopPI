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

import java.io.IOException;
import java.nio.BufferOverflowException;
import java.util.Map;

public class FileCollectorWithIDMapper extends FileCollector {
	private final Map<Integer, String> map;
	
	public FileCollectorWithIDMapper(String path, Map<Integer, String> itemIDmap) throws IOException {
		super(path);
		this.map = itemIDmap;
	}
	
	@Override
	protected void putItem(int i) {
		try {
			byte[] asBytes = this.map.get(i).getBytes(charset);
			buffer.put(asBytes);
		} catch (BufferOverflowException e) {
			flush();
			putItem(i);
		}
	}
}
