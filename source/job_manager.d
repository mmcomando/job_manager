module job_manager;

import core.atomic;
import core.thread:Thread,ThreadID,sleep,Fiber;
import std.algorithm:remove;
import std.conv:to;
import core.cpuid:threadsPerCPU;
import core.sync.mutex;
import std.traits:ReturnType,Parameters,TemplateOf;

import multithreaded_utils;
import universal_delegate;
import cache_vector;
import job_vector;

import std.stdio:write,writeln,writefln;
import std.format:format;
import core.stdc.stdlib;






alias JobVector=LowLockQueue!(JobDelegate*,bool);
//alias JobVector=LockedVector!(JobDelegate*);

alias FiberVector=LowLockQueue!(FiberData,bool);
//alias FiberVector=LockedVector!(FiberData);


uint jobManagerThreadNum;//thread local var
__gshared JobManager jobManager=new JobManager;
class JobManager{
	struct DebugHelper{
		align(64)shared uint jobsAdded;
		align(64)shared uint jobsDone;
		align(64)shared uint fibersAdded;
		align(64)shared uint fibersDone;
		
		void resetCounters(){
			{
				atomicStore(jobsAdded, 0);
				atomicStore(jobsDone, 0);
				atomicStore(fibersAdded, 0);
				atomicStore(fibersDone, 0);
			}
		}
		void jobsAddedUp(){	     atomicOp!"+="(jobsAdded, 1); }
		void jobsDoneUp(){	     atomicOp!"+="(jobsDone, 1);  }
		void fibersAddedUp(){	 atomicOp!"+="(fibersAdded, 1); }
		void fibersDoneUp(){	 atomicOp!"+="(fibersDone, 1);  }

		
		
	}
	DebugHelper debugHelper;
	CacheVector fibersCache;
	//enum CounterSize=__traits( classInstanceSize, Counter);
	//BucketAllocator!(CounterSize) counterAllocator;

	
	//jobs managment
	private JobVector waitingJobs;
	//fibers managment
	private FiberVector[] waitingFibers;
	//thread managment
	private Thread[] threadPool;
	private bool[] threadWorking;
	bool exit;

	void init(){
		init(threadsPerCPU);
	}
	void init(uint threadsCount){
		threadWorking=mallocator.makeArray!(bool)(threadsCount);
		waitingFibers=mallocator.makeArray!(FiberVector)(threadsCount);
		foreach(ref f;waitingFibers)f=mallocator.make!FiberVector;
		threadPool=mallocator.makeArray!(Thread)(threadsCount);
		foreach(i;0..threadsCount){
			Thread th=mallocator.make!Thread(&threadRunFunction);
			th.name=i.to!string;
			threadPool[i]=th;
		}
		waitingJobs=mallocator.make!JobVector();
		fibersCache=mallocator.make!CacheVector(128);
	}
	void start(){
		foreach(thread;threadPool){
			thread.start();
		}
	}
	void waitForEnd(){
		bool wait=true;
		do{
			wait= !waitingJobs.empty;
			foreach(fibers;waitingFibers){
				wait=wait || !fibers.empty ;
			}
			foreach(yes;threadWorking){
				wait=wait || yes;
			}
			foreach(th;threadPool){
				if(!th.isRunning){
					wait=false;
				}
			}
			Thread.sleep(100.msecs);
		}while(wait);
	}
	void end(){
		exit=true;
		foreach(thread;threadPool){
			thread.join;
		}
	}
	
	void addFiber(FiberData fiberData){
		assert(waitingFibers.length==threadPool.length);
		assert(fiberData.fiber.state!=Fiber.State.TERM);
		waitingFibers[fiberData.threadNum].add(fiberData);
		debugHelper.fibersAddedUp();

	}
	void addJob(JobDelegate* del){
		waitingJobs.add(del);
		debugHelper.jobsAddedUp();
	}
	void addJobs(JobDelegate*[] dels){
		foreach(del;dels){
			//waitingJobs.add(del);
			debugHelper.jobsAddedUp();
		}
		waitingJobs.add(dels);
	}

	void runNextJob(){
		static Fiber lastFreeFiber;//1 element tls cache
		enum bool useCache=false;//may be bugged and for simple scenario simple 1 element backup is even faster. I think in real scenario cache should be faster

		Fiber fiber;
		FiberData fd=waitingFibers[jobManagerThreadNum].pop;
		if(fd!=FiberData.init){
			fiber=fd.fiber;
			debugHelper.fibersDoneUp();
		}else if( !waitingJobs.empty ){
			JobDelegate* job;
			job=waitingJobs.pop();
			if(job !is null){
				debugHelper.jobsDoneUp();
				static if(useCache){
					fiber=fibersCache.getData(jobManagerThreadNum,cast(uint)threadWorking.length);
					assert(fiber.state==Fiber.State.TERM);
					fiber.reset(*job);
				}else{
					if(lastFreeFiber is null){
						fiber= mallocator.make!Fiber( *job);
					}else{
						fiber=lastFreeFiber;
						lastFreeFiber=null;
						fiber.reset(*job);
					}
				}
			}	
		}
		//nothing to do
		if(fiber is null ){
			threadWorking[jobManagerThreadNum]=false;
			//Thread.sleep(10.usecs);
			return;
		}
		//do the job
		threadWorking[jobManagerThreadNum]=true;
		assert(fiber.state==Fiber.State.HOLD);
		fiber.call();

		//reuse fiber
		if(fiber.state==Fiber.State.TERM){
			static if(useCache){
				fibersCache.removeData(fiber,jobManagerThreadNum,cast(uint)threadWorking.length);
			}else{
				if(lastFreeFiber !is null){
					mallocator.dispose(lastFreeFiber);
				}
				lastFreeFiber=fiber;
			}
		}
	}
	void threadRunFunction(){
		jobManagerThreadNum=Thread.getThis.name.to!uint;
		threadWorking[jobManagerThreadNum]=true;
		while(!exit){
			runNextJob();
		}
	}
	
}
struct FiberData{
	Fiber fiber;
	uint threadNum;
}
FiberData getFiberData(){
	Fiber fiber=Fiber.getThis();
	assert(fiber !is null);
	return FiberData(fiber,jobManagerThreadNum);
}
struct Counter{
	align (64)shared uint count;
	align (64)FiberData waitingFiber;
	this(uint count){
		this.count=count;
	}
	void decrement(){
		if(waitingFiber.fiber is null){
			return;
		}
		atomicOp!"-="(count, 1);
		bool ok=cas(&count,0,1000);
		if(ok){
			jobManager.addFiber(waitingFiber);
		}
		
	}
}

struct UniversalJob(Delegate){
	alias UnDelegate=UniversalDelegate!Delegate;
	UnDelegate unDel;
	JobDelegate runDel;
	JobDelegate* delPointer;
	Counter* counter;
	void run(){	
		unDel.callAndSaveReturn();
		if(counter.waitingFiber!=counter.waitingFiber.init){
			counter.decrement();
		}
	}

	void init(Delegate del,Parameters!(Delegate) args){
		unDel=makeUniversalDelegate!(Delegate)(del,args);
		runDel=&run;
		delPointer=&runDel;
	}

}

import core.stdc.stdlib;
auto callAndWait(Delegate)(Delegate del,Parameters!(Delegate) args){
	UniversalJob!(Delegate) unJob;
	unJob.init(del,args);
	Counter counter;
	counter.count=1;
	counter.waitingFiber=getFiberData();
	unJob.counter=&counter;
	jobManager.waitingJobs.add(unJob.delPointer);
	jobManager.debugHelper.jobsAddedUp();
	Fiber.yield();
	static if(unJob.unDel.hasReturn){
		return unJob.unDel.result;
	}
}


struct UniversalJobGroup(Delegate){
	alias UnJob=UniversalJob!(Delegate);
	Counter counter;
	uint jobsNum;
	uint jobsAdded;
	UnJob[] unJobs;
	JobDelegate*[] dels;

	this(uint jobsNum){
		this.jobsNum=jobsNum;
	}
	void add(Delegate del,Parameters!(Delegate) args){
		assert(unJobs.length>0 && jobsAdded<jobsNum);
		unJobs[jobsAdded].init(del,args);
		dels[jobsAdded]=unJobs[jobsAdded].delPointer;
		jobsAdded++;
	}
	//returns range so you can allocate it as you want
	//but remember that data is stack allocated
	auto wait(){
		counter.count=jobsNum;
		counter.waitingFiber=getFiberData();
		foreach(ref unDel;unJobs){
			unDel.counter=&counter;
		}
		jobManager.addJobs(dels);
		Fiber.yield();
		import std.algorithm:map;
		static if(UnJob.UnDelegate.hasReturn){
			return unJobs.map!(a => a.unDel.result);
		}

	}
}


//has to be a mixin because the code has to be executed in calling .... (alloca)
string getStackMemory(string varName){
	string code=format(		"
	uint ___jobsNum%s=%s.jobsNum;
	%s.UnJob* ___unJobsMemory%s;
	JobDelegate** ___delsMemory%s;
	bool ___useStack%s=___jobsNum%s<200;

	scope(exit){if(!___useStack%s){
	   free(___unJobsMemory%s);
	   free(___delsMemory%s);
	}}

    if(___useStack%s){
		___unJobsMemory%s=cast(%s.UnJob*)alloca(%s.UnJob.sizeof*___jobsNum%s);//TODO aligment?
		___delsMemory%s=cast(JobDelegate**)alloca((JobDelegate*).sizeof*___jobsNum%s);//TODO aligment?
    }else{
		___unJobsMemory%s=cast(%s.UnJob*)malloc(%s.UnJob.sizeof*___jobsNum%s);//TODO aligment?
		___delsMemory%s=cast(JobDelegate**)malloc((JobDelegate*).sizeof*___jobsNum%s);//TODO aligment?
    }
	%s.unJobs=___unJobsMemory%s[0..___jobsNum%s];
	%s.dels=___delsMemory%s[0..___jobsNum%s];
		",varName,varName,varName,varName,
		varName,varName,varName,varName,
		varName,varName,varName,varName,
		varName,varName,varName,varName,
		varName,varName,varName,varName,
		varName,varName,varName,varName,
		varName,varName,varName,varName,varName);
	return code;

	
}

alias JobDelegate=void delegate();



import std.datetime;
import std.functional:toDelegate;
import std.random:uniform;
import std.algorithm:sum;



void makeTestJobsFrom(void function() fn,uint num){
	makeTestJobsFrom(fn.toDelegate,num);
}
void makeTestJobsFrom(JobDelegate deleg,uint num){
	UniversalJobGroup!JobDelegate group=UniversalJobGroup!JobDelegate(num);
	mixin(getStackMemory("group"));
	foreach(int i;0..num){
		group.add(deleg);
	}
	group.wait();
}

void testFiberLockingToThread(){
	ThreadID id=Thread.getThis.id;
	auto fiberData=getFiberData();
	foreach(i;0..1000){
		jobManager.addFiber(fiberData);
		Fiber.yield();
		assert(id==Thread.getThis.id);
	}
}

//returns how many jobs it have spawned
int randomRecursionJobs(int deepLevel){
	alias UD=UniversalDelegate!(int function(int));
	if(deepLevel==0){
		simpleYield();
		return 0;
	}
	int randNum=uniform(1,10);
	//randNum=10;

	alias ddd=typeof(&randomRecursionJobs);
	UniversalJobGroup!ddd group=UniversalJobGroup!ddd(randNum);
	mixin(getStackMemory("group"));
	foreach(int i;0..randNum){
		group.add(&randomRecursionJobs,deepLevel-1);
	}

	auto jobsRun=group.wait();
	return sum(jobsRun)+randNum;
}
/// One Job and one Fiber.yield
void simpleYield(){
	auto fiberData=getFiberData();
	foreach(i;0..1){
		jobManager.addFiber(fiberData);
		Fiber.yield();
	}
}

void testPerformance(){		
	uint iterations=1000;
	uint packetSize=100;
	StopWatch sw;
	sw.start();
	jobManager.debugHelper.resetCounters();
	alias ddd=typeof(&simpleYield);
	UniversalJobGroup!ddd group=UniversalJobGroup!ddd(packetSize);
	mixin(getStackMemory("group"));
	foreach(int i;0..packetSize){
		group.add(&simpleYield);
	}
	foreach(i;0..iterations){
		group.wait();
	}
	assert(jobManager.debugHelper.jobsAdded==iterations*packetSize);
	assert(jobManager.debugHelper.jobsDone ==iterations*packetSize);
	assert(jobManager.debugHelper.fibersAdded==iterations*packetSize+iterations);
	assert(jobManager.debugHelper.fibersDone ==iterations*packetSize+iterations);
	sw.stop();  
	writefln( "Benchmark: %s*%s : %s[ms], %s[it/ms]",iterations,packetSize,sw.peek().msecs,iterations*packetSize/sw.peek().msecs);
}
import gl3n.linalg;
void mulMat(mat4[] mA,mat4[] mB,mat4[] mC){
	assert(mA.length==mB.length && mB.length==mC.length);
	foreach(i;0..mA.length){
		foreach(k;0..1){
			mC[i]=mB[i]*mB[i];
		}
	}
}
__gshared float result;
__gshared float base=1;
void testPerformanceMatrix(){	
	import std.parallelism;
	uint partsNum=16;
	uint iterations=10000;	
	uint matricesNum=512;
	assert(matricesNum%partsNum==0);
	mat4[] matricesA=mallocator.makeArray!mat4(matricesNum);
	mat4[] matricesB=mallocator.makeArray!mat4(matricesNum);
	mat4[] matricesC=mallocator.makeArray!mat4(matricesNum);
	scope(exit){
		mallocator.dispose(matricesA);
		mallocator.dispose(matricesB);
		mallocator.dispose(matricesC);
	}
	StopWatch sw;
	sw.start();
	jobManager.debugHelper.resetCounters();
	uint step=matricesNum/partsNum;

	alias ddd=typeof(&mulMat);
	UniversalJobGroup!ddd group=UniversalJobGroup!ddd(partsNum);
	mixin(getStackMemory("group"));
	foreach(int i;0..partsNum){
		group.add(&mulMat,matricesA[i*step..(i+1)*step],matricesB[i*step..(i+1)*step],matricesC[i*step..(i+1)*step]);
	}
	foreach(i;0..iterations){
		group.wait();
	}

	
	sw.stop();  
	result=cast(float)iterations*matricesNum/sw.peek().usecs;
	writefln( "BenchmarkMatrix: %s*%s : %s[us], %s[it/us] %s",iterations,matricesNum,sw.peek().usecs,cast(float)iterations*matricesNum/sw.peek().usecs,result/base);
}
void test(uint threadsNum=16){
	import core.memory;
	GC.disable();
	version(DigitalMars){
		import etc.linux.memoryerror;
		//registerMemoryErrorHandler();
	}

	void startTest(){
		alias UnDel=void delegate();
		callAndWait!(UnDel)((&testPerformance).toDelegate);
		callAndWait!(UnDel)((&testPerformanceMatrix).toDelegate);
		makeTestJobsFrom(&testFiberLockingToThread,100);
		
		/*jobManager.debugHelper.resetCounters();
		 int jobsRun=callAndWait!(typeof(&randomRecursionJobs))(&randomRecursionJobs,5);
		 assert(jobManager.debugHelper.jobsAdded==jobsRun+1);
		 assert(jobManager.debugHelper.jobsDone==jobsRun+1);
		 assert(jobManager.debugHelper.fibersAdded==jobsRun+2);
		 assert(jobManager.debugHelper.fibersDone==jobsRun+2);*/
	}
	//writeln("Start JobManager test");
	jobManager.init(threadsNum);
	auto del=(&startTest).toDelegate;
	jobManager.addJob(&del);
	jobManager.start();
	jobManager.waitForEnd();
	//Thread.sleep(2.seconds);
	jobManager.end();
	//writeln("End JobManager test");
}
void testScalability(){
	foreach(i;0..16){
		jobManager=mallocator.make!JobManager;
		scope(exit)mallocator.dispose(jobManager);
		write(i+1," ");
		test(i+1);
		if(i==0)base=result;
	}
}
///main functionality test
///

unittest{
	testScalability();
	
}