﻿module job_manager.job_vector;

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
//algorithm from  http://collaboration.cmc.ec.gc.ca/science/rpn/biblio/ddj/Website/articles/DDJ/2008/0811/081001hs01/081001hs01.html
//By Herb Sutter

class LowLockQueue(T,LockType=bool) {
	//alias T=void*;
private:
	static struct Node {
		this( T val )  {			
			value=val;
		}
		T value;
		align (64)  Node* next;//atomic
	};
	
	shared uint elementsAdded;
	shared uint elementsPopped;
	// for one consumer at a time
	align (64)  Node* first;
	// shared among consumers
	align (64) shared LockType consumerLock;

	// for one producer at a time
	align (64)  Node* last; 
	// shared among producers
	align (64) shared LockType producerLock;//atomic

	
	alias Allocator=BucketAllocator!(Node.sizeof);
	//alias Allocator=MyMallcoator;
	//alias Allocator=MyGcAllcoator;
	Allocator allocator;

public:
	this() {
		allocator=Mallocator.instance.make!Allocator();
		first = last =  allocator.make!(Node)( T.init );		
		producerLock = consumerLock = false;
	}
	~this(){
		Mallocator.instance.dispose(allocator);
	}

	
	bool empty(){
		return (first.next == null); 
	}
	

	
	void add( T  t ) {
		Node* tmp = allocator.make!(Node)( t );
		while( !cas(&producerLock,cast(LockType)false,cast(LockType)true )){ } 	// acquire exclusivity
		last.next = tmp;		 		// publish to consumers
		last = tmp;		 		// swing last forward
		atomicStore(producerLock,false);		// release exclusivity
		atomicOp!"+="(elementsAdded,1);

	}
	void add( T[]  t ) {

		Node* firstInChain;
		Node* lastInChain;
		Node* tmp = allocator.make!(Node)( t[0] );
		firstInChain=tmp;
		lastInChain=tmp;
		foreach(n;1..t.length){
			tmp = allocator.make!(Node)( t[n] );
			lastInChain.next=tmp;
			lastInChain=tmp;
		}
		while( !cas(&producerLock,cast(LockType)false,cast(LockType)true )){ } 	// acquire exclusivity
		last.next = firstInChain;		 		// publish to consumers
		last = lastInChain;		 		// swing last forward
		atomicStore(producerLock,cast(LockType)false);		// release exclusivity
		atomicOp!"+="(elementsAdded,t.length);

	}
	

	
	T pop(  ) {
		while( !cas(&consumerLock,cast(LockType)false,cast(LockType)true ) ) { }	 // acquire exclusivity

		
		Node* theFirst = first;
		Node* theNext = first.next;
		if( theNext != null ) { // if queue is nonempty
			T result = theNext.value;	 	       	// take it out
			theNext.value = T.init; 	       	// of the Node
			first = theNext;		 	       	// swing first forward
			atomicStore(consumerLock,cast(LockType)false);	       	// release exclusivity		
			atomicOp!"+="(elementsPopped,1);

			allocator.dispose(theFirst);
			return result;	 		// and report success
		}

		atomicStore(consumerLock,cast(LockType)false);       	// release exclusivity
		return T.init; 	// report queue was empty
	}
}

import std.functional:toDelegate;
import std.random:uniform;
static int[] tmpArr=[1,1,1,1,1,1];
void testLLQ(){
	shared uint addedElements;
	LowLockQueue!int queue=Mallocator.instance.make!(LowLockQueue!int);
	scope(exit)Mallocator.instance.dispose(queue);

	void testLLQAdd(){
		uint popped;
		foreach(kk;0..1000){
			uint num=uniform(0,1000);
			atomicOp!"+="(addedElements,num+num*6);
			foreach(i;0..num)queue.add(1);
			foreach(i;0..num)queue.add(tmpArr);
			foreach(i;0..num+num*6){
				popped=queue.pop();
				assert(popped==1);
			}
		}
	}
	testMultithreaded((&testLLQAdd).toDelegate,4);
	assert(queue.elementsAdded==addedElements);
	assert(queue.elementsAdded==queue.elementsPopped);
	assert(queue.first.next==null);
	assert(queue.first==queue.last);
}





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


