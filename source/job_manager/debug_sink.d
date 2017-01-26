/**
Module used to efficiently store simple data from many threads.
May be used for validation multithreated algorithms.
Ex. multithreated executes 1000 jobs, each jobs adds to sink unique number.
After execution if in this sink are all 1000 numbers and all are unique everything was ok.
*/

module job_manager.debug_sink;


import std.experimental.allocator;
import std.experimental.allocator.mallocator;

import job_manager.shared_vector;
import job_manager.utils;



import job_manager.vector;

class DebugSink{
	alias T=int;


	alias DataVector=Vector!T;
	static DataVector vector;

	alias DataDataVector=LockedVector!DataVector;
	__gshared DataDataVector allData;


	static this(){
		vector=Mallocator.instance.make!DataVector;
		allData.add(vector);
	}
	static ~this(){
		allData.removeElement(vector);
		Mallocator.instance.dispose(vector);
	}
	
	
	shared static this(){
		allData=Mallocator.instance.make!DataDataVector;
	}
	shared static ~this(){
		Mallocator.instance.dispose(allData);
	}

	static void add(T obj){
		vector~=obj;
	}
	static void reset(){
		foreach(arr;allData){
			arr.reset();
		}
	}
	
	static auto getAll(){
		return allData;
	}
	static verifyUnique(int expectedNum){
		import std.algorithm;
		import std.array;
		auto all=DebugSink.getAll()[];
		auto oneRange=all.map!((a) => a[]).joiner;
		int[] allocated=oneRange.array;
		allocated.sort();
		assertM(allocated.length,expectedNum);
		allocated= allocated[0..allocated.length-allocated.uniq().copy(allocated).length];
		assertM(allocated.length,expectedNum);
	}
}


