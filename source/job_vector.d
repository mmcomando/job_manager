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
//ponoć

class LowLockQueue(T) {
	alias Allocator=BucketAllocator!(Node.sizeof);
	Allocator allocator;
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
	align (64) shared bool consumerLock;

	// for one producer at a time
	align (64)  Node* last; 
	// shared among producers
	align (64) shared bool producerLock;//atomic

	

public:
	this() {
		//allocator=new Allocator();
		//void[] memory=allocator.allocate();
		//first = last =  memory.emplace!(Node)( null );
		first = last = new Node( null );
		producerLock = consumerLock = false;
	}
	~this() {

		/*while( first != null ) {   		// release the list
			Node* tmp = first;
			first = tmp.next;
			//delete tmp.value; 		// no-op if null
			//delete tmp;
		}*/
	}

	bool empty(){
		return (first.next == null); 
	}
	

	
	void add( T  t ) {
		//void[] memory=allocator.allocate();
		//GC.addRange(memory.ptr,memory.length);
		//Node* tmp = memory.emplace!(Node)( t );
		//assertLock(memory.ptr==tmp);
		Node* tmp = new Node(t);
		while( !cas(&producerLock,false,true ))
		{ } 	// acquire exclusivity
		last.next = tmp;		 		// publish to consumers
		last = tmp;		 		// swing last forward
		atomicStore(producerLock,false);		// release exclusivity
	}
	


	T pop(  ) {
		while( !cas(&consumerLock,false,true ) ) 
		{ }	 // acquire exclusivity

		
		Node* theFirst = first;
		Node* theNext = first.next;
		if( theNext != null ) { // if queue is nonempty
			T result = theNext.value;	 	       	// take it out
			theNext.value = null; 	       	// of the Node
			first = theNext;		 	       	// swing first forward
			atomicStore(consumerLock,false);	       	// release exclusivity		

			//result = *val;  		// now copy it back
			//delete val;  		// clean up the value
			//delete theFirst;  		// and the old dummy
			//GC.removeRange(theFirst);
			//allocator.deallocate(cast(void[])theFirst[0..1]);
			return result;	 		// and report success
		}

		atomicStore(consumerLock,false);       	// release exclusivity
		return null; 	// report queue was empty
	}
}








////////////////////
import std.algorithm:remove;


class LockedVector(T){
	//alias T=void*;
	Mutex mutex;
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
			array~=t;
		}
	}
	
	
	
	T pop(  ) {
		synchronized( mutex ){
			if(array.length==0)return null;
			T obj=array[$-1];
			array=array.remove(array.length-1);
			return obj;
		}
	}
	
}