module cache_vector;

import core.stdc.stdlib:malloc,free;
import core.stdc.string:memset,memcpy;
import core.atomic;
import core.sync.mutex;
import std.stdio:writefln,writeln;
import job_manager;
import std.datetime;
import std.random:uniform;

class CacheVector{
	alias T=int;
	static struct DataStruct{
		T data;
		uint numTaken;
		bool used;

	}
	shared DataStruct[] dataArray;
	shared uint loaders;
	shared uint dataGot;
	Mutex mutex;

	this(uint length){
		assert(length>0);
		dataArray=extendArray(length,dataArray);
		//mutex
		mutex=new Mutex();
	}
	void clear(){
		synchronized(mutex){
			atomicStore(dataArray,null);
			while(atomicLoad(loaders)!=0){}
			atomicStore(dataArray,extendArray(1,dataArray));
		}
	}

	void setTo0(){
		synchronized(mutex){
			auto sliceCopy=dataArray;
			atomicStore(dataArray,null);
			while(atomicLoad(loaders)!=0){}
			foreach(ref data;dataArray){
				data.used=false;
				data.data=0;
			}
			atomicStore(dataArray,sliceCopy);
		}
	}
	static shared(DataStruct[])  extendArray(uint length,shared DataStruct[] oldDataArray){
		DataStruct[] array;
		//writefln("a%s | %s",length,DataStruct.sizeof);
		assertLock(length>oldDataArray.length);
		
		//data

		uint size=cast(uint)DataStruct.sizeof*length;
		uint oldSize=cast(uint)(DataStruct.sizeof*oldDataArray.length);
		//writefln("b%s | %s",length,size);
		DataStruct* memory=cast(DataStruct*)malloc(size);
		memset(memory,0,size);//TODO can fill only part
		memcpy(memory,cast(DataStruct*)oldDataArray.ptr,oldSize);
		array=cast(shared DataStruct[])memory[0 .. length];
		if(array.length<6){
			//writeln(oldDataArray);
			//writeln(array);
		}

		//for better crashes
		if(oldDataArray.length!=0){
			memset(cast(DataStruct*)oldDataArray.ptr,0,oldSize);// cast(uint)2863311530 -binary 101010
			free(cast(DataStruct*)oldDataArray.ptr);
		}
		return cast(shared DataStruct[])array;
	}

	T getData(){
		atomicOp!"+="(dataGot, 1);
		//scope(exit)writefln("okk: %d %d",dataArray.length,atomicLoad(dataGot));
		//try to find free slot with atmoics
		{
			atomicOp!"+="(loaders, 1);
			scope(exit)atomicOp!"-="(loaders, 1);
			auto localSlice=atomicLoad(dataArray);
			//writeln(localSlice);
			//localSlice=null;
			foreach(uint i,data;localSlice){
				//writefln("cc: %d %d",dataUsed.length,dataArray.length);
				//assertLock(dataUsed.length==dataArray.length);
				if(localSlice[i].used==false){
					bool isEmpty=cas(&localSlice[i].used,false,true);
					if(isEmpty){
						atomicOp!"+="(localSlice[i].data, 1);
						return cast(T)atomicLoad(localSlice[i].data);
					}
				}
			}
		}
		//try to find free slot with mutex
		synchronized(mutex){
			atomicFence();
			foreach(uint i,ref data;dataArray){
				if(data.used==false){
					data.used=true;
					return cast(T)atomicLoad(dataArray[i].data);
				}
			}
			//all slots used
			//extend array

			//writefln("---");
			//writefln("%s | %s",777,dataUsed.length);
			auto sliceCopy=dataArray;
			//stop all getting data without a lock
			atomicStore(dataArray,null);
			//wait for all loads
			while(atomicLoad(loaders)!=0){}
			//writeln(sliceCopy.length);
			auto newArray=extendArray(cast(uint)sliceCopy.length*2,sliceCopy);
			auto freeIndex=sliceCopy.length;
			newArray[freeIndex].used=true;
			atomicOp!"+="(newArray[freeIndex].data, 10000);
			atomicStore(dataArray,newArray);
			return cast(T)dataArray[freeIndex].data;
		}
	}
}

void testMultithreaded(void delegate() func,uint threadsCount=0){
	threadsCount=threadsPerCPU;
	Thread[] threadPool;
	foreach(i;0..threadsCount){
		Thread th=new Thread(func);
		threadPool~=th;
	}
	foreach(thread;threadPool){
		thread.start();
	}
	foreach(thread;threadPool){
		thread.join();
	}
}

//test extend
unittest{
	/*CacheVector vec=new CacheVector(1);assertLock(vec.dataArray.length==1);
	 vec.extendArray(2,vec.dataArray);assertLock(vec.dataArray.length==2);
	 vec.extendArray(3,vec.dataArray);assertLock(vec.dataArray.length==3);
	 vec.extendArray(4,vec.dataArray);assertLock(vec.dataArray.length==4);
	 vec.extendArray(50,vec.dataArray);assertLock(vec.dataArray.length==50);*/
}

//test extend
unittest{
	/*alias T=void*;
	 version(DigitalMars){
	 import etc.linux.memoryerror;
	 registerMemoryErrorHandler();
	 }
	 CacheVector vec=new CacheVector(10);
	 T var;
	 foreach(i;0..10){
	 var=vec.getData();assertLock(vec.dataArray.length==10);
	 }
	 var=vec.getData();assertLock(vec.dataArray.length==20);*/
}

//test multithreaded
void testCV(){
	version(DigitalMars){
		import etc.linux.memoryerror;
		registerMemoryErrorHandler();
	}
	shared uint sum;
	CacheVector vec=new CacheVector(1);
	void testGet(){
		uint numGot;
		foreach(i;0..uniform(2042,4096)){//64
			vec.clear();
			//vec.setTo0();
			uint rand=uniform(8,64);
			foreach(j;0..rand){//4096
				auto data=vec.getData();
			}
			numGot+=rand;
		}
		atomicOp!"+="(sum, numGot);
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
		foreach(i,data;vec.dataArray){
			writeln(i,data.used);
			assertLock(data.used);
		}
	}
	/*writeln("Start test");
	 jobManager.init(4);
	 jobManager.addJob((&startTest).toDelegate);
	 jobManager.start();
	 jobManager.waitForEnd();
	 jobManager.end();
	 writeln("End test");*/
	StopWatch sw;
	sw.start();
	vec.dataGot=0;
	testMultithreaded(&testGet,16);
	foreach(i,data;vec.dataArray){
		assertLock(data.data==0 || data.data==1 || data.data==10000);
	}
	assert(vec.dataGot==sum);
	sw.stop();  
	writefln( "Benchmark: %s %s[ms], %s[it/ms]",sum,sw.peek().msecs,sum/sw.peek().msecs);
}
unittest{
	testCV();
}