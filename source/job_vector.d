module job_vector;

import core.stdc.stdlib:malloc,free;
import core.stdc.string:memset,memcpy;
import core.atomic;
import core.sync.mutex;
import core.thread:Fiber,Thread;
import core.memory;
import std.conv:emplace;
import core.memory;
import std.experimental.allocator;

import multithreaded_utils;
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
	

	// for one consumer at a time
	align (64)  Node* first;
	// shared among consumers
	align (64) shared LockType consumerLock;

	// for one producer at a time
	align (64)  Node* last; 
	// shared among producers
	align (64) shared LockType producerLock;//atomic

	enum useAllocator=true;
	static if(useAllocator){
		alias Allocator=BucketAllocator!(Node.sizeof);
		Allocator allocator;
	}
public:
	this() {
		static if(useAllocator){
			allocator=mallocator.make!Allocator();
			void[] memory=allocator.allocate();
			first = last =  memory.emplace!(Node)( T.init );
		}else{
			first = last = new Node( T.init );
		}
		producerLock = consumerLock = false;
	}
	~this(){
		static if(useAllocator){
			mallocator.dispose(allocator);
		}
	}

	
	bool empty(){
		return (first.next == null); 
	}
	

	
	void add( T  t ) {
		static if(useAllocator){
			void[] memory=allocator.allocate();
			Node* tmp = memory.emplace!(Node)( t );
		}else{
			//assertLock(memory.ptr==tmp);
			Node* tmp = new Node(t);
		}
		while( !cas(&producerLock,cast(LockType)false,cast(LockType)true )){ } 	// acquire exclusivity
		last.next = tmp;		 		// publish to consumers
		last = tmp;		 		// swing last forward
		atomicStore(producerLock,false);		// release exclusivity
	}
	void add( T[]  t ) {

		Node* firstInChain;
		Node* lastInChain;
		static if(useAllocator){
			void[] memory=allocator.allocate();
			Node* tmp = memory.emplace!(Node)( t[0] );
			firstInChain=tmp;
			lastInChain=tmp;
			foreach(n;1..t.length){
				memory=allocator.allocate();
				tmp = memory.emplace!(Node)( t[n] );
				lastInChain.next=tmp;
				lastInChain=tmp;
			}
		}else{
			Node*[] tmp;
			tmp.length=t.length;
			foreach(i,ref n;tmp)n= new Node(t[i]);
			Node* next;
			foreach_reverse(n;tmp){
				n.next=next;
				next=n;
			}
			firstInChain=tmp[0];
			lastInChain=tmp[$-1];
			
		}
		dummyLoad();
		while( !cas(&producerLock,cast(LockType)false,cast(LockType)true )){ } 	// acquire exclusivity
		last.next = firstInChain;		 		// publish to consumers
		last = lastInChain;		 		// swing last forward
		atomicStore!(MemoryOrder.rel)(producerLock,cast(LockType)false);		// release exclusivity
	}
	

	
	T pop(  ) {
		dummyLoad();
		while( !cas(&consumerLock,cast(LockType)false,cast(LockType)true ) ) { }	 // acquire exclusivity

		
		Node* theFirst = first;
		Node* theNext = first.next;
		if( theNext != null ) { // if queue is nonempty
			T result = theNext.value;	 	       	// take it out
			theNext.value = T.init; 	       	// of the Node
			first = theNext;		 	       	// swing first forward
			atomicStore!(MemoryOrder.rel)(consumerLock,cast(LockType)false);	       	// release exclusivity		

			static if(useAllocator){
				allocator.deallocate(cast(void[])theFirst[0..1]);
			}
			return result;	 		// and report success
		}

		atomicStore!(MemoryOrder.rel)(consumerLock,cast(LockType)false);       	// release exclusivity
		return T.init; 	// report queue was empty
	}
}








////////////////////
import std.algorithm:remove;

class LockedVectorBuildIn(T){
	align(64)Mutex mutex;
	T[] array;
public:
	this(){
		mutex=new Mutex;
	}
	bool empty(){
		return(array.length==0);
	}	
	
	void add( T  t ) {
		synchronized( mutex ){
			array.assumeSafeAppend~=t;
		}
	}	
	void add( T[]  t ) {
		synchronized( mutex ){
			array.assumeSafeAppend~=t;
		}
	}
	
	T pop(  ) {
		synchronized( mutex ){
			if(array.length==0)return T.init;
			T obj=array[$-1];
			array=array.remove(array.length-1);
			return obj;
		}
	}
	
}

class LockedVector(T){
	align(64)Mutex mutex;
	Vector!T array;
public:
	this(){
		mutex=mallocator.make!(Mutex);
		array=mallocator.make!(Vector!T);
	}
	~this(){
		mallocator.dispose(mutex);
		mallocator.dispose(array);
	}
	bool empty(){
		return(array.length==0);
	}	
	
	void add( T  t ) {
		synchronized( mutex ){
			array~=t;
		}
	}	
	void add( T[]  t ) {
		synchronized( mutex ){
			array~=t;
		}
	}
	
	T pop(  ) {
		synchronized( mutex ){
			if(array.length==0)return T.init;
			T obj=array[$-1];
			array.remove(array.length-1);
			return obj;
		}
	}
	
}


class Vector(T){
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
		T[] oldArray=array;
		size_t oldSize=oldArray.length*T.sizeof;
		size_t newSize=newNumOfElements*T.sizeof;
		T* memory=cast(T*)malloc(newSize);
		memcpy(cast(void*)memory,cast(void*)oldArray.ptr,oldSize);
		array=memory[0..newNumOfElements];

		if(oldArray !is null){
			memset(cast(void*)oldArray.ptr,0,oldArray.length*T.sizeof);
			free(cast(void*)oldArray.ptr);
		}
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