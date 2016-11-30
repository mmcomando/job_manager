module job_manager;

import std.datetime;
import core.atomic;
import core.thread:Thread,ThreadID,sleep,Fiber;
import std.algorithm:remove;
import std.conv:to;
import core.cpuid:threadsPerCPU;
import core.sync.mutex;
import std.traits:ReturnType,Parameters;
import universal_delegate;

import std.stdio:write,writeln,writefln;

uint threadNum;//thread local var
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
		while(1){
			writefln("assert failed, thread: %s file: %s line: %s",Thread.getThis.id,file,line);
			Thread.sleep(1000.msecs);
		}
	}
}

__gshared JobManager jobManager=new JobManager;
class JobManager{
	//for debug
	shared uint jobsAdded;
	shared uint jobsDone;
	shared uint fibersAdded;
	shared uint fibersDone;

	//jobs managment
	private Job[] waitingJobs;
	private Mutex waitingJobsMutex;
	//fibers managment
	private FiberData[][] waitingFibers;
	private Mutex[] waitingFibersMutex;
	//thread managment
	private Thread[] threadPool;
	private bool[] threadWorking;
	bool exit;
	
	void resetCounters(){
		atomicStore(jobsAdded, 0);
		atomicStore(jobsDone, 0);
		atomicStore(fibersAdded, 0);
		atomicStore(fibersDone, 0);
		
	}
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
		waitingFibersMutex.length=threadsCount;
		foreach(ref mut;waitingFibersMutex)mut=new Mutex;
		waitingJobsMutex=new Mutex;
	}
	void start(){
		foreach(thread;threadPool){
			thread.start();
		}
	}
	void waitForEnd(){
		bool wait=true;
		do{
			wait=waitingJobs.length>0;
			foreach(fibers;waitingFibers){
				wait=wait || fibers.length>0;
			}
			foreach(yes;threadWorking){
				wait=wait || yes;
			}
			Thread.sleep(100.msecs);
		}while(wait);
		writeln("Wait ended");
	}
	void end(){
		exit=true;
		foreach(thread;threadPool){
			thread.join;
		}
	}
	
	void addFiber(FiberData fiberData){
		assertLock(waitingFibers.length==threadPool.length);
		synchronized( waitingFibersMutex[fiberData.threadNum] )
		{
			assertLock(fiberData.fiber.state!=Fiber.State.TERM);
			waitingFibers[fiberData.threadNum].assumeSafeAppend~=fiberData;
			atomicOp!"+="(fibersAdded, 1);
		}
	}
	void addJob(JobDelegate del){
		Job job=new Job;
		job.del=del;
		synchronized( waitingJobsMutex )
		{
			waitingJobs~=job;
			atomicOp!"+="(jobsAdded, 1);
		}
	}
	
	
	void addJobAndWait(JobDelegate del){
		assertLock(Fiber.getThis() !is null);
		Counter counter=new Counter;
		Job job=new Job;
		job.del=del;
		counter.count=1;
		job.counter=counter;
		counter.waitingFiber=getFiberData();
		synchronized( waitingJobsMutex )
		{
			waitingJobs~=job;
			atomicOp!"+="(jobsAdded, 1);
		}
		Fiber.yield();
	}
	void addJobAndWait(JobDelegate[] dels){
		if(dels.length==0)return;
		assertLock(Fiber.getThis() !is null);
		Counter counter=new Counter;
		Job[] jobs;
		jobs.length=dels.length;
		counter.count=cast(uint)dels.length;
		counter.waitingFiber=getFiberData();
		synchronized( waitingJobsMutex )
		{
			foreach(i;0..dels.length){
				jobs[i]=new Job;
				jobs[i].del=dels[i];
				jobs[i].counter=counter;
				waitingJobs~=jobs[i];
				atomicOp!"+="(jobsAdded, 1);
			}
		}
		Fiber.yield();
	}
	//synchronized by calling function
	private FiberData popFiber(){
		FiberData ff;
		foreach(i,fiberData;waitingFibers[threadNum]){
			ff=waitingFibers[threadNum][i];
			waitingFibers[threadNum]=waitingFibers[threadNum].remove(i);
			atomicOp!"+="(fibersDone, 1);
			break;
		}
		
		return ff;
	}
	//synchronized by calling function
	private Job popJob(){
		Job ff=waitingJobs[$-1];
		waitingJobs=waitingJobs.remove(waitingJobs.length-1);
		atomicOp!"+="(jobsDone, 1);
		return ff;

	}
	void runNextJob(){
		static Fiber lastFreeFiber;
		Fiber fiber;
		Job job;
		synchronized( waitingFibersMutex[threadNum] )
		{
			if(waitingFibers[threadNum].length>0){
				fiber=popFiber().fiber;
			}
		}
		synchronized( waitingJobsMutex )
		{
			if(fiber is null && waitingJobs.length>0){
				job=popJob();
				if(lastFreeFiber is null){
					fiber= new Fiber( &job.run);
				}else{
					fiber=lastFreeFiber;
					lastFreeFiber=null;
					fiber.reset(&job.run);
				}
			}
		}
		if(fiber is null ){
			threadWorking[threadNum]=false;
			return;
		}
		threadWorking[threadNum]=true;
		assertLock(fiber.state==Fiber.State.HOLD);
		fiber.call();	
		if(fiber.state==Fiber.State.TERM){
			lastFreeFiber=fiber;
		}
	}
	void threadRunFunction(){
		threadNum=Thread.getThis.name.to!uint;
		threadWorking[threadNum]=true;
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
	auto addJobAndWait(Delegate)(Delegate[] unDels){
		JobDelegate[] dels;
		dels.length=unDels.length;
		foreach(i,ref del;dels){
			del=&unDels[i].callAndSaveReturn;
		}
		addJobAndWait(dels);
		alias RetType=ReturnType!(Delegate.deleg);
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
	return FiberData(fiber,threadNum);
}
class Counter{
	
	FiberData waitingFiber;
	shared uint count;
	shared uint countUp;
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

alias JobDelegate=void delegate();
class Job{
	JobDelegate del;
	Counter counter;
	void run(){	
		del();
		if(counter !is null){
			counter.decrement();
		}
	}
	
}



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
	//dels.length=10;
	dels.length=randNum;
	foreach(ref d;dels){
		d=makeUniversalDelegate!(typeof(&randomRecursionJobs))(&randomRecursionJobs,deepLevel-1);
	}
	int[] jobsRun=jobManager.addJobAndWait(dels);
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
	uint iterations=100;
	uint packetSize=10_000;
	StopWatch sw;
	sw.start();
	jobManager.resetCounters();
	JobDelegate[] dels=makeTestJobsFrom(&simpleYield,packetSize);
	foreach(i;0..iterations){
		jobManager.addJobAndWait(dels);
	}
	writeln(jobManager.jobsAdded);
	writeln(jobManager.jobsDone);
	writeln(jobManager.fibersAdded);
	writeln(jobManager.fibersDone);
	assertLock(jobManager.jobsAdded==iterations*packetSize);
	assertLock(jobManager.jobsDone ==iterations*packetSize);
	assertLock(jobManager.fibersAdded==iterations*packetSize+iterations);
	assertLock(jobManager.fibersDone ==iterations*packetSize+iterations);
	sw.stop();  
	writefln( "Benchmark: %s*%s : %s[ms]",iterations,packetSize,sw.peek().msecs);
}
void test(){
	import core.memory;
	//GC.disable();
	version(DigitalMars){
		import etc.linux.memoryerror;
		registerMemoryErrorHandler();
	}
	
	void startTest(){
		JobDelegate[] dels;
		
		jobManager.addJobAndWait((&testPerformance).toDelegate);
		
		dels=makeTestJobsFrom(&testFiberLockingToThread,100);
		jobManager.addJobAndWait(dels);
		
		jobManager.resetCounters();
		int jobsRun=jobManager.addJobAndWait!(typeof(&randomRecursionJobs))(&randomRecursionJobs,5);
		assertLock(jobManager.jobsAdded==jobsRun+1);
		assertLock(jobManager.jobsDone==jobsRun+1);
		assertLock(jobManager.fibersAdded==jobsRun+2);
		assertLock(jobManager.fibersDone==jobsRun+2);
		
	}
	writeln("Start test");
	jobManager.init(16);
	jobManager.addJob((&startTest).toDelegate);
	jobManager.start();
	jobManager.waitForEnd();
	jobManager.end();
	writeln("End test");
}
///main functionality test
unittest{
	//test();
	
}