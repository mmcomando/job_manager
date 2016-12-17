﻿module multithreaded_utils;

import core.thread;
import core.cpuid:threadsPerCPU;
import std.stdio:writeln,writefln;
import std.conv:to;
import std.random:uniform;
import job_manager;
import job_vector;
import std.experimental.allocator.building_blocks;
public import std.experimental.allocator:make,makeArray,dispose;
shared Mallocator mallocator;
import core.bitop;
shared static this(){
	mallocator=Mallocator.instance;
}

size_t nextPow2(size_t num){
	return 1<< bsr(num)+1;
}

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
	static immutable Exception exc=new Exception("Dummy");
	try{
		throw exc;
	}catch(Exception e ){
		printException(e);
	}
}


//useful for testing if function is safe in multthreated enviroment
//name can be used as id
void testMultithreaded(void delegate() func,uint threadsCount=0){
	if(threadsCount==0)
		threadsCount=threadsPerCPU;
	Thread[] threadPool=mallocator.makeArray!(Thread)(threadsCount);
	foreach(i;0..threadsCount){
		Thread th=mallocator.make!Thread(func);
		th.name=i.to!string;//maybe there is better way to pass data to a thread?
		threadPool[i]=th;
	}
	foreach(thread;threadPool)thread.start();
	foreach(thread;threadPool)thread.join();
	foreach(thread;threadPool)mallocator.dispose(thread);
	mallocator.dispose(threadPool);

	
}


import core.atomic;
import core.sync.mutex;
import  std.random:uniform;



class BucketAllocator(uint bucketSize){
	static assert(bucketSize>=8);
	enum shared Bucket* invalidValue=cast(shared Bucket*)858567;

	static struct Bucket{
		union{
			void[bucketSize] data;
			Bucket* next;
		}
	}
	enum bucketsNum=128;
	Mutex mutex;

	
	static struct BucketsArray{
		Bucket[bucketsNum] buckets;
		shared Bucket* empty;
		void initialize() shared {
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

	 Vector!(shared BucketsArray*) bucketArrays;

	this(){
		bucketArrays=mallocator.make!(Vector!(shared BucketsArray*));
		mutex=mallocator.make!(Mutex);
		extend();
	}

	~this(){
		mallocator.dispose(mutex);
	}
	void extend(){
		shared BucketsArray* arr=cast(shared BucketsArray*)mallocator.make!(BucketsArray);
		(*arr).initialize();
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
		assert(0);
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
	BucketAllocator!(64) allocator=mallocator.make!(BucketAllocator!(64));
	scope(exit)mallocator.dispose(allocator);
	shared ulong sum;
	void test(){
		foreach(k;0..100){
			void[][] memories;
			uint rand=uniform(130,140);
			memories=mallocator.makeArray!(void[])(rand);
			scope(exit)mallocator.dispose(memories);
			foreach(i;0..rand){
				memories[i]=allocator.allocate();
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