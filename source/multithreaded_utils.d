module multithreaded_utils;

import core.thread;
import core.cpuid:threadsPerCPU;
import std.stdio:writeln,writefln;
import std.conv:to;
import std.random:uniform;
import job_manager;



void printException(Exception e, int maxStack = 4) {
	writeln("Exception message: ", e.msg);
	writefln("File: %s Line Number: %s Thread: %s", e.file, e.line,Thread.getThis.id);
	writeln("Call stack:");
	foreach (i, b; e.info) {
		writeln(b);
		if (i >= maxStack)
			break;
	}
	writeln("--------------");
}
void printStack(){
	try{
		throw new Exception("Dummy");
	}catch(Exception e ){
		printException(e);
	}
}

//simple assert stopped/killed?? thread and printed nothing so not very useful
void assertLock(bool ok,string file=__FILE__,int line=__LINE__){
	if(!ok){
		printStack();
		while(1){
			writefln("assert failed, thread: %s file: %s line: %s",Thread.getThis.id,file,line);
			Thread.sleep(1000.msecs);
		}
	}
}
uint dummyNum=0;
uint dummySum;
void dummyLoad(){
	//foreach(i;0..jobManagerThreadNum*dummyNum){
	//	dummySum+=uniform(0,12331);
	//}
}
//useful for testing if function is safe in multthreated enviroment
//name can be used as id
void testMultithreaded(void delegate() func,uint threadsCount=0){
	if(threadsCount==0)
		threadsCount=threadsPerCPU;
	Thread[] threadPool;
	foreach(i;0..threadsCount){
		Thread th=new Thread(func);
		th.name=i.to!string;//maybe there is better way to pass data to a thread?
		threadPool~=th;
	}
	foreach(thread;threadPool){
		thread.start();
	}
	foreach(thread;threadPool){
		thread.join();
	}

}

unittest{
	int[] a=[];
	/*writeln(a.iterateWithRandom);
	writeln(a.iterateWithRandom);
	writeln(a.iterateWithRandom);
	writeln(a.iterateWithRandom);
	writeln(a.iterateWithRandom);
	writeln(a.iterateWithRandom);*/
}

import core.atomic;
import core.sync.mutex;
import  std.random:uniform;



class BucketAllocator(uint bucketSize){
	static assert(bucketSize>=8);
	enum shared Bucket* invalidValue=cast(shared Bucket*)858567;

	struct Bucket{
		union{
			void[bucketSize] data;
			Bucket* next;
		}
	}
	enum bucketsNum=1282;
	Mutex mutex;


	static struct BucketsArray{
		Bucket[bucketsNum] buckets;
		shared Bucket* empty;
		void init() shared {
			shared Bucket* last;
			foreach(i,ref bucket;buckets){
				bucket.next=last;
				last=&bucket;
			}
			empty=cast(shared Bucket*)last;
		}
		uint freeSlots()shared {
			uint i;
			shared Bucket* slot=empty;
			while(slot !is null){
				i++;
				slot=slot.next;
			}
			return i;
		}
		uint usedSlots()shared {
			return bucketsNum-freeSlots;
		}
	}

	shared BucketsArray*[] bucketArrays;

	this(){
		mutex=new Mutex;
		extend();
	}
	void extend(){
		shared BucketsArray* arr=new shared BucketsArray();
		(*arr).init();
		bucketArrays~=arr;
	}

	void[] allocate(){
	FF:foreach(bucketsArray;bucketArrays){
			if(bucketsArray.empty is null)continue;

			shared Bucket* emptyBucket;
			do{
			BACK:
				emptyBucket=atomicLoad(bucketsArray.empty);
				if(emptyBucket is null){
					continue FF;
				}
				if(emptyBucket==invalidValue){
					goto BACK;
				}
			}while(!cas(&bucketsArray.empty,emptyBucket,invalidValue));
			atomicStore(bucketsArray.empty,emptyBucket.next);
			return cast(void[])emptyBucket.data;
		}
		//assert(0);
		synchronized(mutex){
			extend();
			auto bucketsArray=bucketArrays[$-1];
			shared Bucket* empty=bucketsArray.empty;
			bucketsArray.empty=(*bucketsArray.empty).next;
			return 	cast(void[])empty.data;		
		}

	}
	void deallocate(void[] data){
		foreach(bucketsArray;bucketArrays){
			auto ptr=bucketsArray.buckets.ptr;
			auto dptr=data.ptr;
			if(dptr>=ptr+bucketsNum || dptr<ptr){
				continue;
			}
			shared Bucket* bucket=cast(shared Bucket*)data.ptr;
			shared Bucket* emptyBucket;

			do{
			BACK:
				emptyBucket=atomicLoad(bucketsArray.empty);
				if(emptyBucket==invalidValue){
					goto BACK;
				}
				bucket.next=emptyBucket;
			}while(!cas(&bucketsArray.empty,emptyBucket,bucket));
			return;
		}
		assertLock(0);
	}

	uint usedSlots(){
		uint sum;
		foreach(bucketsArray;bucketArrays)sum+=bucketsArray.usedSlots;
		return sum;

	}
}



unittest{
	BucketAllocator!(64) allocator=new BucketAllocator!(64);
	foreach(k;0..123){
		void[][] memories;
		assert(allocator.bucketArrays[0].freeSlots==allocator.bucketsNum);
		foreach(i;0..allocator.bucketsNum){
			memories~=allocator.allocate();
		}
		assert(allocator.bucketArrays[0].freeSlots==0);
		foreach(i;0..allocator.bucketsNum){
			memories~=allocator.allocate();
			assert(allocator.bucketArrays.length==2);
		}
		foreach(i,m;memories){
			allocator.deallocate(m);
		}
	}

}
import std.datetime;
void testAL(){
	BucketAllocator!(64) allocator=new BucketAllocator!(64);
	shared ulong sum;
	void test(){
		foreach(k;0..100){
			void[][] memories;
			foreach(i;0..uniform(130,140)){
				memories~=allocator.allocate();
			}
			foreach(m;memories){
				allocator.deallocate(m);
			}
			atomicOp!"+="(sum,memories.length);
		}
	}
	void testAdd(){
		foreach(i;0..128){
			allocator.allocate();
		}
	}
	StopWatch sw;
	sw.start();
	testMultithreaded(&test,16);
	sw.stop();  	
	writefln( "Benchmark: %s %s[ms], %s[it/ms]",sum,sw.peek().msecs,sum/sw.peek().msecs);

	assert(allocator.usedSlots==0);
}
unittest{
	//testAL();
}