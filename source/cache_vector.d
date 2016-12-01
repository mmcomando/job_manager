module cache_vector;

import core.stdc.stdlib:malloc,free;
import core.stdc.string:memset,memcpy;
import core.atomic;
import core.sync.mutex;
import std.stdio:writefln,writeln;
import job_manager;
import std.datetime;
import std.random:uniform,randomShuffle;
import std.conv:to;
import core.thread;
import core.memory;

class CacheVector{
	alias T=Fiber;
	static  struct DataStruct{ 
			bool used;
			T data;		
	}
	alias LoType=ulong;
	align (64)shared DataStruct[] dataArray;
	align (64)shared LoType loaders;
	shared uint dataGot;
	shared uint dataRemoved;
	Mutex mutex;

	this(uint length){
		assert(length>0);
		dataArray=extendArray(length,dataArray);
		mutex=new Mutex();
	}
	void clear(){
		synchronized(mutex){
			atomicStore(dataArray,null);
			int i;
			while(atomicLoad(loaders)!=0){
			}
			atomicStore(dataArray,extendArray(1,dataArray));
		}
	}
	T initVar(){
		static void dummy(){}
		auto fiber=new Fiber(&dummy);
		fiber.call();
		atomicOp!"+="(dataGot, 1);
		assertLock(fiber.state==Fiber.State.TERM);
		return fiber;
	}
	shared(DataStruct[])  extendArray(uint length,shared DataStruct[] oldDataArray){
		DataStruct[] array;
		//writefln("a%s | %s",length,DataStruct.sizeof);
		assertLock(length>oldDataArray.length);
		
		//data

		uint size=cast(uint)DataStruct.sizeof*length;
		uint oldSize=cast(uint)(DataStruct.sizeof*oldDataArray.length);
		//writefln("b%s | %s",length,size);
		DataStruct* memory=cast(DataStruct*)malloc(size);
		GC.addRange(memory, size);
		memset(memory,0,size);//TODO can fill only part
		memcpy(memory,cast(DataStruct*)oldDataArray.ptr,oldSize);
		array=cast(shared DataStruct[])memory[0 .. length];

		foreach(ref data;array[oldDataArray.length .. length]){
			data.data=initVar();
		}
		//if(array.length<80){
		//	writeln("-",cast(void*)this);
			//foreach(a;array){writeln("|",cast(void*)a.data);}
		//}

		//for better crashes
		if(oldDataArray.length!=0){
			memset(cast(DataStruct*)oldDataArray.ptr,0,oldSize);// cast(uint)2863311530 -binary 101010
			free(cast(DataStruct*)oldDataArray.ptr);
			GC.removeRange(cast(DataStruct*)oldDataArray.ptr);
		}
		return cast(shared DataStruct[])array;
	}

	T getData(uint thread=0,uint threadCount=1){
		//atomicOp!"+="(dataGot, 1);
		//scope(exit)writefln("okk: %d %d",dataArray.length,atomicLoad(dataGot));
		//try to find free slot with atmoics
		{
			atomicOp!"+="(loaders, cast(LoType)1);
			scope(exit)atomicOp!"-="(loaders, cast(LoType)1);
			auto localSlice=atomicLoad(dataArray);
			uint division=(cast(uint)localSlice.length/threadCount)*thread;
			//writeln(localSlice);
			//localSlice=null;
			foreach(uint i,ref data;localSlice[division..$]){
				if(data.used==false){
					bool isEmpty=cas(&data.used,false,true);
					if(isEmpty){
						//atomicOp!"+="(localSlice[i].data, 1);
						//return cast(T)atomicLoad(data.data);
						return cast(Fiber)data.data;
					}
				}
			}
			foreach(uint i,ref data;localSlice[0..division]){
				if(data.used==false){
					bool isEmpty=cas(&data.used,false,true);
					if(isEmpty){
						//atomicOp!"+="(localSlice[i].data, 1);
						//return cast(T)atomicLoad(data.data);
						return cast(Fiber)data.data;
					}
				}
			}
		}
		//try to find free slot with mutex
		synchronized(mutex){
			//atomicFence();
			foreach(uint i,ref data;dataArray){
				if(data.used==false){
					data.used=true;
					return cast(T)atomicLoad(dataArray[i].data);
				}
			}
			//all slots used
			//extend array
			auto sliceCopy=dataArray;
			//stop all getting data without a lock
			atomicStore(dataArray,null);
			//wait for all loads
			int i;
			while(atomicLoad(loaders)!=0){
			}

			auto newArray=extendArray(cast(uint)sliceCopy.length*2,sliceCopy);
			auto freeIndex=sliceCopy.length;
			newArray[freeIndex].used=true;
			//atomicOp!"+="(newArray[freeIndex].data, 10000);
			atomicStore(dataArray,newArray);
			return cast(T)newArray[freeIndex].data;
		}
	}

	void removeData(T elementToDelete,uint thread=0,uint threadCount=1){
		//atomicOp!"+="(dataRemoved, 1);
		{
			atomicOp!"+="(loaders, cast(LoType)1);
			scope(exit)atomicOp!"-="(loaders, cast(LoType)1);
			auto localSlice=atomicLoad(dataArray);
			uint division=(cast(uint)localSlice.length/threadCount)*thread;
			foreach(uint i,ref data;localSlice[division..$]){
				if(cast(Fiber)data.data==elementToDelete){
					atomicStore(data.used, false);
					return;
				}
			}
			foreach(uint i,ref data;localSlice[0..division]){
				if(cast(Fiber)data.data==elementToDelete){
					atomicStore(data.used, false);
					return;
				}
			}
		}
		synchronized(mutex){
			auto localSlice=atomicLoad(dataArray);
			foreach(uint i,ref data;localSlice){
				if(cast(Fiber)data.data==elementToDelete){
					atomicStore(data.used, false);
					return;
				}
			}
		}
	}
}
void testMultithreaded(void delegate() func,uint threadsCount=0){
	if(threadsCount==0)
		threadsCount=threadsPerCPU;
	Thread[] threadPool;
	foreach(i;0..threadsCount){
		Thread th=new Thread(func);
		th.name=i.to!string;
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
	CacheVector vec=new CacheVector(1);assertLock(vec.dataArray.length==1);
	vec.dataArray=vec.extendArray(2,vec.dataArray);assertLock(vec.dataArray.length==2);
	vec.dataArray=vec.extendArray(3,vec.dataArray);assertLock(vec.dataArray.length==3);
	vec.dataArray=vec.extendArray(4,vec.dataArray);assertLock(vec.dataArray.length==4);
	vec.dataArray=vec.extendArray(50,vec.dataArray);assertLock(vec.dataArray.length==50);
}

//test extend
unittest{
	/*alias T=Fiber;
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
void stupid(){
	//writeln("stupid");
}
//test multithreaded
void testCV(){
	version(DigitalMars){
		import etc.linux.memoryerror;
		registerMemoryErrorHandler();
	}
	shared uint sum;
	CacheVector vec=new CacheVector(1);
	immutable uint firstLoop=10000;
	immutable uint secondLoop=8;
	void testGet(){
		uint threadNum=Thread.getThis.name.to!uint;
		//writeln(threadNum);
		uint numGot;
		foreach(i;0..firstLoop){
			vec.clear();
			//vec.setTo0();
			uint rand=secondLoop;
			foreach(j;0..rand){
				auto data=vec.getData();
			}
			numGot+=rand;
		}
		atomicOp!"+="(sum, numGot);
	}
	void testGet2(){
		uint threadNum=Thread.getThis.name.to!uint;
		uint numGot;
		foreach(i;0..firstLoop){
			vec.clear();
			//vec.setTo0();
			uint rand=secondLoop;
			foreach(j;0..rand){
				auto data=vec.getData(threadNum,16);
			}
			numGot+=rand;
		}
		atomicOp!"+="(sum, numGot);
	}
	void testRemove(){
		uint threadNum=Thread.getThis.name.to!uint;
		uint numGot;
	Fiber[secondLoop] arr;
		foreach(i;0..firstLoop){
			uint rand=secondLoop;
			foreach(j;0..rand){
				arr.ptr[j]=vec.getData();
			}
			randomShuffle(arr[0..rand]);
			foreach(j;0..rand){
				vec.removeData(arr.ptr[j]);
			}
			numGot+=rand;
		}
		atomicOp!"+="(sum, numGot);
	}
	void testRemove2(){
		uint threadNum=Thread.getThis.name.to!uint;
		uint numGot;
		Fiber[secondLoop] arr;
		int[] ddd;
		foreach(i;0..firstLoop){
			uint rand=secondLoop;
			foreach(j;0..rand){
				arr.ptr[j]=vec.getData(threadNum,16);
			}
			foreach(j;0..rand){
				foreach(k;0..100){
					ddd~=1;
				arr.ptr[j].reset(&stupid);
				arr.ptr[j].call();
				}
			}
			randomShuffle(arr[0..rand]);
			foreach(j;0..rand){
				vec.removeData(arr.ptr[j],threadNum,16);
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
			assertLock(data.used!=0);
		}
	}
	/*writeln("Start test");
	 jobManager.init(4);
	 jobManager.addJob((&startTest).toDelegate);
	 jobManager.start();
	 jobManager.waitForEnd();
	 jobManager.end();
	 writeln("End test");*/

	/*{ 
		StopWatch sw;
		sw.start();
		vec.dataGot=0;
		testMultithreaded(&testGet,16);
		sw.stop();
		assert(vec.dataGot==sum);
		  
		writefln( "G1 Benchmark: %s %s[ms], %s[it/ms]",vec.dataGot,sw.peek().msecs,vec.dataGot/sw.peek().msecs);
		vec.clear();
	}

	{ 
		StopWatch sw;
		sw.start();
		sum=0;
		vec.dataGot=0;
		testMultithreaded(&testGet2,16);
		sw.stop();  
		assert(vec.dataGot==sum);

		writefln( "G2 Benchmark: %s %s[ms], %s[it/ms]",vec.dataGot,sw.peek().msecs,vec.dataGot/sw.peek().msecs);
		vec.clear();
	}
	{
		StopWatch sw;
		sw.start();
		sum=0;
		vec.dataGot=0;
		vec.dataRemoved=0;
		testMultithreaded(&testRemove,16);
		sw.stop();  
		foreach(i,data;vec.dataArray){
			assertLock(!data.used);
		}
		writefln( "R1 Benchmark: %s %s[ms], %s[it/ms]",sum,sw.peek().msecs,sum/sw.peek().msecs);
	}*/
	{
		//GC.disable;
		StopWatch sw;
		sw.start();
		sum=0;
		vec.dataGot=0;
		vec.dataRemoved=0;
		testMultithreaded(&testRemove2,16);
		sw.stop();  
		foreach(i,data;vec.dataArray){
			writeln(cast(void*)data.data);
			assertLock(!data.used);
		}
		writeln(vec.dataGot);
		writeln(vec.dataArray.length);
		writefln( "R2 Benchmark: %s %s[ms], %s[it/ms]",sum,sw.peek().msecs,sum/sw.peek().msecs);
	}
}
unittest{
	//testCV();
	//writeln(CacheVector.DataStruct.data.offsetof);
	//writeln(CacheVector.DataStruct.used.offsetof);
	//writeln(CacheVector.DataStruct.sizeof);
}