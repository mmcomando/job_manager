module job_vector;

import core.stdc.stdlib:malloc,free;
import core.stdc.string:memset,memcpy;
import core.atomic;
import core.sync.mutex;
import core.thread:Fiber,Thread;
import core.memory;
import std.conv:emplace;
import core.memory;

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
			allocator=new Allocator();
			void[] memory=allocator.allocate();
			first = last =  memory.emplace!(Node)( T.init );
		}else{
			first = last = new Node( T.init );
		}
		producerLock = consumerLock = false;
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


class LockedVector(T){
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
	
	T pop(  ) {
		synchronized( mutex ){
			if(array.length==0)return T.init;
			T obj=array[$-1];
			array=array.remove(array.length-1);
			return obj;
		}
	}
	
}