module job_manager.job_vector;

import core.stdc.stdlib:malloc,free;
import core.stdc.string:memset,memcpy;
import core.atomic;
import core.thread:Fiber,Thread;
import core.memory;
import std.conv:emplace;
import core.memory;
import std.experimental.allocator;

import job_manager.multithreaded_utils;
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
		allocator=mallocator.make!Allocator();
		first = last =  allocator.make!(Node)( T.init );		
		producerLock = consumerLock = false;
	}
	~this(){
		mallocator.dispose(allocator);
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
	LowLockQueue!int queue=mallocator.make!(LowLockQueue!int);
	scope(exit)mallocator.dispose(queue);

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

class LockedVector(T){
	Vector!T array;
public:
	this(){
		array=mallocator.make!(Vector!T);
	}
	~this(){
		mallocator.dispose(array);
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
	
	T pop(  ) {
		synchronized( this ){
			if(array.length==0)return T.init;
			T obj=array[$-1];
			array.remove(array.length-1);
			return obj;
		}
	}
	
}


class Vector(T){
@nogc:
	T[] array;
	size_t used;
public:
	this(size_t numElements=1){
		assert(numElements>0);
		extend(numElements);
	}
	bool empty(){
		return (used==0);
	}
	size_t length(){
		return used;
	}	
	void reserve(size_t numElements){
		if(numElements>array.length){
			extend(numElements);
		}
	}

	void extend(size_t newNumOfElements){
		auto oldArray=manualExtend(newNumOfElements);
		if(oldArray !is null){
			freeData(oldArray);
		}
	}
	void[] manualExtend(size_t newNumOfElements=0){
		if(newNumOfElements==0)newNumOfElements=array.length*2;
		T[] oldArray=array;
		size_t oldSize=oldArray.length*T.sizeof;
		size_t newSize=newNumOfElements*T.sizeof;
		//T[] memory=mallocator.makeArray!(T)(newNumOfElements);
		//memcpy(cast(void*)memory.ptr,cast(void*)oldArray.ptr,oldSize);
		//array=memory;
		T* memory=cast(T*)malloc(newSize);
		memcpy(cast(void*)memory,cast(void*)oldArray.ptr,oldSize);
		array=memory[0..newNumOfElements];
		return cast(void[])oldArray;
		
	}
	bool canAddWithoutRealloc(uint elemNum=1){
		return used+elemNum<=array.length;
	}

	void add( T  t ) {
		if(used>=array.length){
			extend(array.length*2);
		}
		array[used]=t;
		used++;
	}

	void add( T[]  t ) {
		if(used+t.length>array.length){
			extend(nextPow2(used+t.length));
		}
		foreach(i;0..t.length){
			array[used+i]=t[i];
		}
		used+=t.length;
	}
	void remove(size_t elemNum){
		array[elemNum]=array[used-1];
		used--;
	}

	T opIndex(size_t elemNum){
		assert(elemNum<used);
		return array[elemNum];
	}
	auto opSlice(){
		return array[0..used];
	}
	size_t opDollar(){
		return used;
	}
	void opOpAssign(string op)(T obj){
		static assert(op=="~");
		add(obj);
	}
	void opOpAssign(string op)(T[] obj){
		static assert(op=="~");
		add(obj);
	}
	void opIndexAssign(T obj,size_t elemNum){
		assert(elemNum<used);
		array[elemNum]=obj;

	}
	
}
unittest{
	Vector!int vec=new Vector!int;
	assert(vec.empty);
	vec.add(0);
	vec.add(1);
	vec.add(2);
	vec.add(3);
	vec.add(4);
	vec.add(5);
	assert(vec.length==6);
	assert(vec[3]==3);
	assert(vec[5]==5);
	assert(vec[]==[0,1,2,3,4,5]);
	assert(!vec.empty);
	vec.remove(3);
	assert(vec.length==5);
	assert(vec[]==[0,1,2,5,4]);//unstable remove

}

unittest{
	Vector!int vec=new Vector!int;
	assert(vec.empty);
	vec~=[0,1,2,3,4,5];
	assert(vec[]==[0,1,2,3,4,5]);
	assert(vec.length==6);
	vec~=6;
	assert(vec[]==[0,1,2,3,4,5,6]);
	
}


unittest{
	Vector!int vec=new Vector!int;
	vec~=[0,1,2,3,4,5];
	vec[3]=33;
	assert(vec[3]==33);
	
}