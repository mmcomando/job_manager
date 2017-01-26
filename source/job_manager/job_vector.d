module job_manager.job_vector;

import core.stdc.stdlib:malloc,free;
import core.stdc.string:memset,memcpy;
import core.atomic;
import core.thread:Fiber,Thread;
import core.memory;
import std.conv:emplace;
import core.memory;
import std.experimental.allocator;
import std.experimental.allocator.mallocator;

import job_manager.shared_allocator;
import job_manager.shared_utils;
import job_manager.utils;

//import job_manager.multithreaded_utils;





////////////////////
import std.algorithm:remove;

class LockedVectorBuildIn(T){
	T[] array;
public:
	bool empty(){
		return(array.length==0);
	}	
	
	void add( T  t ) {
		synchronized( this ){
			array.assumeSafeAppend~=t;
		}
	}	
	void add( T[]  t ) {
		synchronized( this ){
			array.assumeSafeAppend~=t;
		}
	}
	
	T pop(  ) {
		synchronized( this ){
			if(array.length==0)return T.init;
			T obj=array[$-1];
			array=array.remove(array.length-1);
			return obj;
		}
	}
	
}

import job_manager.vector;
class LockedVector(T){
	Vector!T array;
public:
	this(){
		array=Mallocator.instance.make!(Vector!T);
	}
	~this(){
		Mallocator.instance.dispose(array);
	}
	bool empty(){
		return(array.length==0);
	}	
	
	void add( T  t ) {
		synchronized( this ){
			array~=t;
		}
	}	
	void add( T[]  t ) {
		synchronized( this ){
			array~=t;
		}
	}
	void removeElement( T elem ) {
		synchronized( this ){
			array.removeElement(elem);
		}
	}
	
	T pop(  ) {
		synchronized( this ){
			if(array.length==0)return T.init;
			T obj=array[$-1];
			array.remove(array.length-1);
			return obj;
		}
	}
	auto opSlice(){
		return array[];
	}
	
}


class SharedSink{
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
		atomicFence();
		import std.algorithm;
		import std.array;
		auto all=SharedSink.getAll()[];
		auto oneRange=all.map!((a) => a[]).joiner;
		int[] allocated=oneRange.array;
		allocated.sort();
		assertM(allocated.length,expectedNum);
		allocated.length -= allocated.uniq().copy(allocated).length;
		assertM(allocated.length,expectedNum);
	}
}


