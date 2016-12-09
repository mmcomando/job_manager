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


alias JobVector=LowLockQueue!(Job*);
//alias JobVector=LockedVector!(Job*);

//alias FiberVector=LowLockQueue!(FiberData);
alias FiberVector=LockedVector!(FiberData);


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
		threadWorking.length=threadsCount;
		foreach(i;0..threadsCount){
			Thread th=new Thread(&threadRunFunction);
			th.name=i.to!string;
			threadPool~=th;
		}
		waitingFibers.length=threadsCount;
		foreach(ref f;waitingFibers)f=new FiberVector;
		waitingJobs=new JobVector();
		fibersCache=new CacheVector(threadsCount);
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
			Thread.sleep(1000.msecs);
		}while(wait);
	}
	void end(){
		exit=true;
		foreach(thread;threadPool){
			thread.join;
		}
	}
	
	void addFiber(FiberData fiberData){
		assertLock(waitingFibers.length==threadPool.length);
		assertLock(fiberData.fiber.state!=Fiber.State.TERM);
		waitingFibers[fiberData.threadNum].add(fiberData);
		debugHelper.fibersAddedUp();

	}
	void addJob(JobDelegate del){
		Job* job=new Job;
		job.del=del;
		waitingJobs.add(job);
		debugHelper.jobsAddedUp();
	}
	
	
	void addJobAndWait(JobDelegate del){
		assertLock(Fiber.getThis() !is null);
		Counter counter=new Counter;
		Job* job=new Job;
		job.del=del;
		counter.count=1;
		job.counter=counter;
		counter.waitingFiber=getFiberData();
		waitingJobs.add(job);
		debugHelper.jobsAddedUp();
		Fiber.yield();
	}
	void addJobsAndWait(JobDelegate[] dels){
		if(dels.length==0)return;
		assertLock(Fiber.getThis() !is null);
		Counter counter=new Counter;
		Job*[] jobs;
		jobs.length=dels.length;
		counter.count=cast(uint)dels.length;
		counter.waitingFiber=getFiberData();

		foreach(i;0..dels.length){
			jobs[i]=new Job;
			jobs[i].del=dels[i];
			jobs[i].counter=counter;
			waitingJobs.add(jobs[i]);
			debugHelper.jobsAddedUp();
		}
		//waitingJobs.add(jobs);//faster? slower?

		Fiber.yield();
	}

	void runNextJob(){
		static Fiber lastFreeFiber;//1 element tls cache
		immutable bool useCache=false;//may be bugged and for simple scenario simple 1 element cache is even faster. I think in real scenario cache should be faster

		Fiber fiber;
		FiberData fd=waitingFibers[jobManagerThreadNum].pop;
		if(fd!=FiberData.init){
			fiber=fd.fiber;
			debugHelper.fibersDoneUp();
		}
		if(fiber is null && !waitingJobs.empty){
			Job* job;
			job=waitingJobs.pop();
			if(job !is null){
				debugHelper.jobsDoneUp();
				if(useCache){
					fiber=fibersCache.getData(jobManagerThreadNum,cast(uint)threadWorking.length);
					assertLock(fiber.state==Fiber.State.TERM);
					fiber.reset(&job.run);
				}else{
					if(lastFreeFiber is null){
						fiber= new Fiber( &job.run);
					}else{
						fiber=lastFreeFiber;
						lastFreeFiber=null;
						fiber.reset(&job.run);
					}
				}
			}	
		}
		//nothing to do
		if(fiber is null ){
			threadWorking[jobManagerThreadNum]=false;
			Thread.sleep(1000.usecs);
			return;
		}
		//do the job
		threadWorking[jobManagerThreadNum]=true;
		assertLock(fiber.state==Fiber.State.HOLD);
		fiber.call();

		//reuse fiber
		if(fiber.state==Fiber.State.TERM){
			if(useCache){
				fibersCache.removeData(fiber,jobManagerThreadNum,cast(uint)threadWorking.length);
			}else{
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
	
	
	
	
	//////////////////////////////////////////////////////////////////////////////////////////
	//normal: delegate,function
	auto addJobAndWait(Delegate)(Delegate del,Parameters!(Delegate) args){
		auto unDel=makeUniversalDelegate!(Delegate)(del,args);
		addJobAndWait(&unDel.callAndSaveReturn);
		static if(!is(ReturnType!Delegate==void)){ 
			return unDel.result;
		}
	}
	//UniversalDelegate
	auto addJobsAndWait(UnDelegate)(UnDelegate[] unDels){
		static assert(__traits(isSame, TemplateOf!(UnDelegate), UniversalDelegate));
		JobDelegate[] dels;
		dels.length=unDels.length;
		foreach(i,ref del;dels){
			del=&unDels[i].callAndSaveReturn;
		}
		addJobsAndWait(dels);
		alias RetType=ReturnType!(UnDelegate.deleg);
		static if(!is(RetType==void)){ 
			RetType[] results;
			results.length=unDels.length;
			foreach(i,ref del;unDels){
				results[i]=del.result;
			}
			return results;
		}
	}
	//////////////////////////////////////////////////////////////////////////////////////////
	
}
struct FiberData{
	Fiber fiber;
	uint threadNum;
}
FiberData getFiberData(){
	Fiber fiber=Fiber.getThis();
	assertLock(fiber !is null);
	return FiberData(fiber,jobManagerThreadNum);
}
class Counter{
	align (64)shared uint count;
	FiberData waitingFiber;
	this(){}
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

struct UniversalJob{
	alias Delegate=int delegate(ref int,string,ulong);
	alias UnDelegate=UniversalDelegate!Delegate;
	Job job;
	UnDelegate unDel;

	void init(Delegate del,Parameters!(Delegate) args){
		unDel=makeUniversalDelegate!(Delegate)(del,args);
		job.del=&unDel.callAndSaveReturn;
	}
}

alias JobDelegate=void delegate();
struct Job{
	JobDelegate del;
	Counter counter;
	void run(){	
		del();
		if(counter !is null){
			counter.decrement();
		}
	}
	
}



import std.datetime;
import std.functional:toDelegate;
import std.random:uniform;
import std.algorithm:sum;



JobDelegate[] makeTestJobsFrom(void function() fn,uint num){
	return makeTestJobsFrom(fn.toDelegate,num);
}
JobDelegate[] makeTestJobsFrom(JobDelegate deleg,uint num){
	JobDelegate[] dels;
	dels.length=num;
	foreach(uint i,ref del;dels){
		del=deleg;
	}
	return dels;
}

void testFiberLockingToThread(){
	ThreadID id=Thread.getThis.id;
	auto fiberData=getFiberData();
	foreach(i;0..1000){
		jobManager.addFiber(fiberData);
		Fiber.yield();
		assertLock(id==Thread.getThis.id);
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
	UD[] dels;
	dels.length=randNum;
	foreach(ref d;dels){
		d=makeUniversalDelegate!(typeof(&randomRecursionJobs))(&randomRecursionJobs,deepLevel-1);
	}
	int[] jobsRun=jobManager.addJobsAndWait(dels);
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
import gl3n.linalg;
void mulMat(mat4[] mA,mat4[] mB,mat4[] mC){
	assertLock(mA.length==mB.length && mB.length==mC.length);
	foreach(i;0..mA.length){
		foreach(k;0..100){
			mC[i]=mB[i]*mB[i];
		}
	}
}
void testPerformance(){		
	uint iterations=100;
	uint packetSize=10_000;
	StopWatch sw;
	sw.start();
	jobManager.debugHelper.resetCounters();
	JobDelegate[] dels=makeTestJobsFrom(&simpleYield,packetSize);
	foreach(i;0..iterations){
		jobManager.addJobsAndWait(dels);
	}
	assertLock(jobManager.debugHelper.jobsAdded==iterations*packetSize);
	assertLock(jobManager.debugHelper.jobsDone ==iterations*packetSize);
	assertLock(jobManager.debugHelper.fibersAdded==iterations*packetSize+iterations);
	assertLock(jobManager.debugHelper.fibersDone ==iterations*packetSize+iterations);
	sw.stop();  
	writefln( "Benchmark: %s*%s : %s[ms], %s[it/ms]",iterations,packetSize,sw.peek().msecs,iterations*packetSize/sw.peek().msecs);
}

void testPerformanceMatrix(){	
	import std.parallelism;
	uint partsNum=32;
	uint iterations=100;	
	uint matricesNum=512;
	assertLock(matricesNum%partsNum==0);
	mat4[] matricesA=new mat4[matricesNum];
	mat4[] matricesB=new mat4[matricesNum];
	mat4[] matricesC=new mat4[matricesNum];
	StopWatch sw;
	sw.start();
	jobManager.debugHelper.resetCounters();
	alias UD=UniversalDelegate!(void function(mat4[],mat4[],mat4[]));
	UD[] dels;
	dels.length=partsNum;
	uint step=matricesNum/partsNum;
	foreach(i,ref d;dels){
		d=makeUniversalDelegate!(typeof(&mulMat))(&mulMat,matricesA[i*step..(i+1)*step],matricesB[i*step..(i+1)*step],matricesC[i*step..(i+1)*step]);
	}
	foreach(i;0..iterations){
		jobManager.addJobsAndWait(dels);
	}
	sw.stop();  
	writefln( "BenchmarkMatrix: %s*%s : %s[ms], %s[it/ms]",iterations,matricesNum,sw.peek().msecs,iterations*matricesNum/sw.peek().msecs);
}
void test(uint threadsNum=16){
	import core.memory;
	//GC.disable();
	version(DigitalMars){
		import etc.linux.memoryerror;
		//registerMemoryErrorHandler();
	}
	
	void startTest(){
		JobDelegate[] dels;
		//jobManager.addJobAndWait((&testPerformanceMatrix).toDelegate);
		jobManager.addJobAndWait((&testPerformance).toDelegate);

		dels=makeTestJobsFrom(&testFiberLockingToThread,100);
		jobManager.addJobsAndWait(dels);
		
		jobManager.debugHelper.resetCounters();
		int jobsRun=jobManager.addJobAndWait!(typeof(&randomRecursionJobs))(&randomRecursionJobs,5);
		assertLock(jobManager.debugHelper.jobsAdded==jobsRun+1);
		assertLock(jobManager.debugHelper.jobsDone==jobsRun+1);
		assertLock(jobManager.debugHelper.fibersAdded==jobsRun+2);
		assertLock(jobManager.debugHelper.fibersDone==jobsRun+2);	
	}
	//writeln("Start JobManager test");
	jobManager.init(threadsNum);
	jobManager.addJob((&startTest).toDelegate);
	//writeln(jobManager.waitingJobs.classinfo.init.length);
	//writeln(jobManager.waitingJobs.classinfo.init.length);
	jobManager.start();
	jobManager.waitForEnd();
	jobManager.end();
	//writeln("End JobManager test");
}
void testScalability(){
	foreach(i;0..16){
		jobManager=new JobManager;
		write(i+1," ");
		test(i+1);
	}
}
///main functionality test
unittest{
	test();
	
}