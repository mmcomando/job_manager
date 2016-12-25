module job_manager;

import core.atomic;
import core.cpuid:threadsPerCPU;
import core.stdc.stdlib;
import core.stdc.string:memset;
import core.thread:Thread,ThreadID,sleep,Fiber;
import std.algorithm:remove;
import std.conv:to;
import std.format:format;
import std.stdio:write,writeln,writefln;
import std.traits:ReturnType,Parameters,TemplateOf;



import cache_vector;
import job_vector;
import multithreaded_utils;
import universal_delegate;


//alias JobVector=LowLockQueue!(JobDelegate*,bool);
//alias JobVector=LockedVector!(JobDelegate*);
alias JobVector=LockedVectorBuildIn!(JobDelegate*);

//alias FiberVector=LowLockQueue!(FiberData,bool);
//alias FiberVector=LockedVector!(FiberData);
alias FiberVector=LockedVectorBuildIn!(FiberData);


uint jobManagerThreadNum;//thread local var
__gshared JobManager jobManager=new JobManager;

alias JobDelegate=void delegate();
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
		void jobsAddedAdd  (int num=1){	 atomicOp!"+="(jobsAdded,  num); }
		void jobsDoneAdd   (int num=1){	 atomicOp!"+="(jobsDone,   num); }
		void fibersAddedAdd(int num=1){	 atomicOp!"+="(fibersAdded,num); }
		void fibersDoneAdd (int num=1){	 atomicOp!"+="(fibersDone, num); }

		
		
	}
	DebugHelper debugHelper;
	
	//jobs managment
	private JobVector waitingJobs;
	//fibers managment
	private FiberVector[] waitingFibers;
	//thread managment
	private Thread[] threadPool;
	private bool[] threadWorking;
	bool exit;

	void init(uint threadsCount=0){
		if(threadsCount==0)threadsCount=threadsPerCPU;
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
		fibersCache=mallocator.make!CacheVector();
	}
	void start(){
		foreach(thread;threadPool){
			thread.start();
		}
	}
	void startMainLoop(void function() mainLoop,uint threadsCount=0){
		startMainLoop(mainLoop.toDelegate,threadsCount);
	}
	void startMainLoop(JobDelegate mainLoop,uint threadsCount=0){
		shared bool endLoop=false;
		static struct NoGcDelegateHelper
		{
			this(JobDelegate del,ref shared bool end){
				this.del=del;
				endPointer=&end;
			}
			JobDelegate del;
			shared bool* endPointer;
			void call() { 
				del();
				atomicStore(*endPointer,true);			
			}
		}
		NoGcDelegateHelper helper=NoGcDelegateHelper(mainLoop,endLoop);
		init(threadsCount);
		auto del=&helper.call;
		addJob(&del);
		start();
		waitForEnd(endLoop);
		end();
	}

	void waitForEnd(ref shared bool end){
		bool wait=true;
		do{
			wait= !atomicLoad(end);
			foreach(th;threadPool){
				if(!th.isRunning){
					wait=false;
				}
			}
			Thread.sleep(10.msecs);
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
		debugHelper.fibersAddedAdd();

	}
	void addJob(JobDelegate* del){
		waitingJobs.add(del);
		debugHelper.jobsAddedAdd();
	}
	void addJobs(JobDelegate*[] dels){
		debugHelper.jobsAddedAdd(cast(int)dels.length);
		waitingJobs.add(dels);
	}

	alias CacheVector=FiberNoCache;
	CacheVector fibersCache;
	uint fibersMade;

	Fiber allocateFiber(JobDelegate del){
		Fiber fiber;
		fiber=fibersCache.getData(jobManagerThreadNum,cast(uint)threadWorking.length);
		assert(fiber.state==Fiber.State.TERM);
		fiber.reset(del);
		fibersMade++;
		return fiber;
	}
	void deallocateFiber(Fiber fiber){
		fibersCache.removeData(fiber,jobManagerThreadNum,cast(uint)threadWorking.length);
	}

	void runNextJob(){
		
		Fiber fiber;
		FiberData fd=waitingFibers[jobManagerThreadNum].pop;
		if(fd!=FiberData.init){
			fiber=fd.fiber;
			debugHelper.fibersDoneAdd();
		}else if( !waitingJobs.empty ){
			JobDelegate* job;
			job=waitingJobs.pop();
			if(job !is null){
				debugHelper.jobsDoneAdd();
				fiber=allocateFiber(*job);
			}	
		}
		//nothing to do
		if(fiber is null ){
			threadWorking[jobManagerThreadNum]=false;
			return;
		}
		//do the job
		threadWorking[jobManagerThreadNum]=true;
		assert(fiber.state==Fiber.State.HOLD);
		fiber.call();

		//reuse fiber
		if(fiber.state==Fiber.State.TERM){
			deallocateFiber(fiber);
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
		assert(count<9000);
		atomicOp!"-="(count, 1);
		bool ok=cas(&count,0,10000);
		if(ok){
			jobManager.addFiber(waitingFiber);
			waitingFiber.fiber=null;
		}

		
	}
}

struct UniversalJob(Delegate){
	alias UnDelegate=UniversalDelegate!Delegate;
	UnDelegate unDel;//user function to run, with parameters and return value
	JobDelegate runDel;//wraper to decrement counter on end
	JobDelegate* delPointer;
	Counter* counter;
	void run(){	
		unDel.callAndSaveReturn();
		if(counter !is null && counter.waitingFiber!=counter.waitingFiber.init){
			counter.decrement();
		}
	}

	void initialize(Delegate del,Parameters!(Delegate) args){
		unDel=makeUniversalDelegate!(Delegate)(del,args);
		runDel=&run;
		delPointer=&runDel;
	}

}

auto callAndWait(Delegate)(Delegate del,Parameters!(Delegate) args){
	UniversalJob!(Delegate) unJob;
	unJob.initialize(del,args);
	Counter counter;
	counter.count=1;
	counter.waitingFiber=getFiberData();
	unJob.counter=&counter;
	jobManager.waitingJobs.add(unJob.delPointer);
	jobManager.debugHelper.jobsAddedAdd();
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
		unJobs[jobsAdded].initialize(del,args);
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

///allocates memory for UniversalJobGroup
///has to be a mixin because the code has to be executed in calling scope (alloca)
string getStackMemory(string varName){
	string code=format(		"
	uint ___jobsNum%s=%s.jobsNum;
	
	%s.unJobs=mallocator.makeArray!(%s.UnJob)(___jobsNum%s);
	%s.dels=mallocator.makeArray!(JobDelegate*)(___jobsNum%s);

	

    scope(exit){
       memset(%s.unJobs.ptr,0,%s.UnJob.sizeof*___jobsNum%s);
       memset(%s.dels.ptr,0,(JobDelegate*).sizeof*___jobsNum%s);
	   mallocator.dispose(%s.unJobs);
	   mallocator.dispose(%s.dels);
	}


		",varName,varName,varName,varName,
		varName,varName,varName,varName,
		varName,varName,varName,varName,
		varName,varName);
	return code;
	
	
}
string getStackMemory2(string varName){
	string code=format(		"
	uint ___jobsNum%s=%s.jobsNum;
	%s.UnJob* ___unJobsMemory%s;
	JobDelegate** ___delsMemory%s;
	bool ___useStack%s=false;//___jobsNum%s<200;

	
    if(___useStack%s){
		___unJobsMemory%s=cast(%s.UnJob*)alloca(%s.UnJob.sizeof*___jobsNum%s);//TODO aligment?
		___delsMemory%s=cast(JobDelegate**)alloca((JobDelegate*).sizeof*___jobsNum%s);//TODO aligment?
    }else{
		___unJobsMemory%s=cast(%s.UnJob*)malloc(%s.UnJob.sizeof*___jobsNum%s);//TODO aligment?
		___delsMemory%s=cast(JobDelegate**)malloc((JobDelegate*).sizeof*___jobsNum%s);//TODO aligment?
    }

    scope(exit){if(!___useStack%s){
       memset(___unJobsMemory%s,0,%s.UnJob.sizeof*___jobsNum%s);
       memset(___delsMemory%s,0,(JobDelegate*).sizeof*___jobsNum%s);
	   free(___unJobsMemory%s);
	   free(___delsMemory%s);
	}}


	%s.unJobs=___unJobsMemory%s[0..___jobsNum%s];
	%s.dels=___delsMemory%s[0..___jobsNum%s];
		",varName,varName,varName,varName,
		varName,varName,varName,varName,
		varName,varName,varName,varName,
		varName,varName,varName,varName,
		varName,varName,varName,varName,
		varName,varName,varName,varName,
		varName,varName,varName,varName,
		varName,varName,varName,varName,
		varName,varName);
	return code;
	
	
}




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
import core.memory;
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
import core.simd;
alias mat4=float[16];
//import gl3n.linalg;
void mulMat(mat4[] mA,mat4[] mB,mat4[] mC){
	assert(mA.length==mB.length && mB.length==mC.length);
	foreach(i;0..mA.length){
		foreach(k;0..1){
			mC[i]=mB[i][]*mB[i][];
		}
	}
}
__gshared float result;
__gshared float base=1;
void testPerformanceMatrix(){	
	import std.parallelism;
	uint partsNum=16;
	uint iterations=100;	
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

	
	void startTest(){
		alias UnDel=void delegate();
		foreach(i;0..1)
			callAndWait!(UnDel)((&testPerformance).toDelegate);
		foreach(i;0..1)
			callAndWait!(UnDel)((&testPerformanceMatrix).toDelegate);
		makeTestJobsFrom(&testFiberLockingToThread,100);

		foreach(i;0..10000){
			int[] pp=	new int[1000];
			jobManager.debugHelper.resetCounters();
			int jobsRun=callAndWait!(typeof(&randomRecursionJobs))(&randomRecursionJobs,5);
			assert(jobManager.debugHelper.jobsAdded==jobsRun+1);
			assert(jobManager.debugHelper.jobsDone==jobsRun+1);
			writeln(atomicLoad(jobManager.debugHelper.fibersAdded)," | ",jobsRun+2);
			writeln(atomicLoad(jobManager.debugHelper.fibersDone)," | ",jobsRun+2);
			assert(jobManager.debugHelper.fibersAdded==jobsRun+2);
			assert(jobManager.debugHelper.fibersDone==jobsRun+2);
		}
	}
	//writeln("Start JobManager test");
	jobManager.startMainLoop(&startTest,threadsNum);
	//writeln("End JobManager test");
}
void testScalability(){
	foreach(i;4..8){
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
