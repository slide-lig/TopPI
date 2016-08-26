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
package fr.liglab.mining.mapred;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class MiningMapper extends Mapper<IntWritable, IntWritable, IntWritable, IntWritable> {
	
	private Grouper grouper;
	private IntWritable keyW = new IntWritable();

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		
		Configuration conf = context.getConfiguration();
		int nbGroups = conf.getInt(TopPIoverHadoop.KEY_NBGROUPS, 1);
		int maxId = conf.getInt(TopPIoverHadoop.KEY_REBASING_MAX_ID, 1);
		
		grouper = new Grouper(nbGroups, maxId);
	}
	
	@Override
	protected void map(IntWritable key, IntWritable value, Context context) throws IOException, InterruptedException {
		
		this.keyW.set(this.grouper.getGroupId(value.get()));
		context.write(this.keyW, value);
	}
}
