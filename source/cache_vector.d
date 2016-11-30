module cache_vector;

import core.stdc.stdlib:malloc,free;
import core.stdc.string:memset,memcpy;
import core.atomic;
import core.sync.mutex;
import std.stdio:writefln,writeln;
import job_manager;

class CacheVector{
	alias T=void*;
	shared T[] dataArray;
	shared bool[] dataUsed;
	shared uint loaders;
	shared uint dataGot;
	Mutex mutex;

	this(uint length){
		extendArray(length,dataArray,dataUsed);
		//mutex
		mutex=new Mutex();
	}
	void extendArray(uint length,shared T[] oldDataArray,shared bool[] oldDataUsed){
		//writefln("%s | %s",length,dataUsed.length);
		assertLock(length>dataUsed.length);
	
		//data

		uint size=cast(uint)T.sizeof*length;
		uint oldSize=cast(uint)(T.sizeof*oldDataArray.length);
		T* memory=cast(T*)malloc(size);
		memset(memory,0,size);//TODO can fill only part
		memcpy(memory,cast(T*)oldDataArray.ptr,oldSize);
		dataArray=cast(shared T[])memory[0 .. length];

		//used

		uint sizeBool=bool.sizeof*length;
		uint oldSizeBool=cast(uint)(bool.sizeof*oldDataUsed.length);
		bool* memoryBool=cast(bool*)malloc(sizeBool);

		memset(memoryBool,0,sizeBool);
		memcpy(memoryBool,cast(bool*)oldDataUsed.ptr,oldSizeBool);
		dataUsed=cast(shared bool[])memoryBool[0 .. length];


		//for better crashes
		if(oldDataUsed.length!=0){
			memset(cast(T*)oldDataArray.ptr,0,oldSize);// cast(uint)2863311530 -binary 101010
			memset(cast(bool*)oldDataUsed.ptr,0,oldSizeBool);
			free(cast(T*)oldDataArray.ptr);
			free(cast(T*)oldDataUsed.ptr);
		}
	}

	T getData(){
		atomicOp!"+="(dataGot, 1);
		//scope(exit)writefln("okk: %d %d",dataArray.length,atomicLoad(dataGot));
		//try to find free slot with atmoics
		{
			atomicOp!"+="(loaders, 1);
			scope(exit)atomicOp!"-="(loaders, 1);
			foreach(uint i,data;atomicLoad(dataArray)){
				//writefln("cc: %d %d",dataUsed.length,dataArray.length);
				//assertLock(dataUsed.length==dataArray.length);
				if(dataUsed[i]==false){
					bool isEmpty=cas(&dataUsed[i],false,true);
					if(isEmpty){
						return cast(T)atomicLoad(dataArray[i]);
					}
				}
			}
		}
		//try to find free slot with mutex
		synchronized(mutex){
			atomicFence();
			foreach(uint i,data;dataArray){
				if(dataUsed[i]==false){
					dataUsed[i]=true;
					return cast(T)atomicLoad(dataArray[i]);
				}
			}
			//all slots used
			//extend array

			//writefln("---");
			//writefln("%s | %s",777,dataUsed.length);
			auto sliceCopy=dataArray;
			//stop all getting data without a lock
			dataArray=null;
			//wait for all loads
			while(atomicLoad(loaders)!=0){
			}
			extendArray(cast(uint)sliceCopy.length*2,sliceCopy,dataUsed);
			auto freeIndex=sliceCopy.length;
			dataUsed[freeIndex]=true;
			return cast(T)dataArray[freeIndex];
		}
	}
}

//test extend
unittest{
	CacheVector vec=new CacheVector(1);assertLock(vec.dataArray.length==1);
	vec.extendArray(2,vec.dataArray,vec.dataUsed);assertLock(vec.dataArray.length==2);
	vec.extendArray(3,vec.dataArray,vec.dataUsed);assertLock(vec.dataArray.length==3);
	vec.extendArray(4,vec.dataArray,vec.dataUsed);assertLock(vec.dataArray.length==4);
	vec.extendArray(50,vec.dataArray,vec.dataUsed);assertLock(vec.dataArray.length==50);
}

//test extend
unittest{
	alias T=void*;
	version(DigitalMars){
		import etc.linux.memoryerror;
		//	registerMemoryErrorHandler();
	}
	CacheVector vec=new CacheVector(10);
	T var;
	foreach(i;0..10){
		var=vec.getData();assertLock(vec.dataArray.length==10);
	}
	var=vec.getData();assertLock(vec.dataArray.length==20);
}

//test multithreaded
unittest{
	CacheVector vec=new CacheVector(1);
	void testGet(){
		foreach(i;0..512){
			auto data=vec.getData();
			/*writeln("--");
			writeln(jobManager.jobsAdded);
			writeln(jobManager.jobsDone);
			writeln(jobManager.fibersAdded);
			writeln(jobManager.fibersDone);*/
		}
	}
	void startTest(){
		JobDelegate[] dels;
		dels=makeTestJobsFrom(&testGet,128);	
		jobManager.resetCounters();
		jobManager.addJobAndWait(dels);	
		writeln("--");
		writeln(vec.dataArray.length);
		writeln(vec.dataGot);
		assertLock(vec.dataArray.length==512*128);
		foreach(i,used;vec.dataUsed){
			writeln(i,used);

			assertLock(used);

		}
	}
	writeln("Start test");
	jobManager.init(16);
	jobManager.addJob((&startTest).toDelegate);
	jobManager.start();
	jobManager.waitForEnd();
	jobManager.end();
	writeln("End test");
}