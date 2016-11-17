module job_manager;
import std.datetime;
import core.atomic;
import core.thread:Thread,ThreadID,sleep,Fiber;
import std.algorithm:remove;
import std.conv:to;
import core.cpuid:threadsPerCPU;
import std.stdio;
import core.sync.mutex;
import std.functional:toDelegate;

uint threadNum;

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
	shared uint jobsAdded;
	shared uint jobsDone;
	shared uint fibersAdded;
	shared uint fibersDone;
	shared uint threadNumCounter;
	private Job[] waitingJobs;
	private FiberData[] waitingFibers;
	private Thread[] threadPool;
	private bool[] threadWorking;
	bool exit;
	Mutex waitingFibersMutex;
	Mutex waitingJobsMutex;

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
		waitingFibersMutex=new Mutex;
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
			wait=waitingJobs.length>0 || waitingFibers.length>0;
			foreach(yes;threadWorking){
				wait=wait || yes;
			}
			Thread.sleep(100.msecs);
		}while(wait);
		while(waitingJobs.length>0 || waitingFibers.length>0){
			Thread.sleep(100.msecs);
		}
		stdout.flush;
		writeln("Wait ended");
	}
	void end(){
		exit=true;
		foreach(thread;threadPool){
			thread.join;
		}
	}
	void addRunningFiberAndWait(){
		assertLock(Fiber.getThis() !is null);
		auto fiberData=getFiberData();
		synchronized( waitingFibersMutex )
		{
			assertLock(fiberData.fiber.state!=Fiber.State.TERM);
			waitingFibers.assumeSafeAppend~=fiberData;
			atomicOp!"+="(fibersAdded, 1);
		}
		Fiber.yield();
	}
	void addFiber(FiberData fiberData){
		synchronized( waitingFibersMutex )
		{
			assertLock(fiberData.fiber.state!=Fiber.State.TERM);
			waitingFibers~=fiberData;
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
	FiberData popFiber(){
		ThreadID myPid=Thread.getThis.id;
		FiberData ff;
		synchronized( waitingFibersMutex )
		{
			//	writeln("ll ",waitingFibers.length);
			foreach(i,fiberData;waitingFibers){
				if(fiberData.pid==myPid){
					ff=waitingFibers[i];
					waitingFibers=waitingFibers.remove(i);
					atomicOp!"+="(fibersDone, 1);
					break;
				}
			}
		}

		return ff;
	}
	Job popJob(){
		synchronized( waitingJobsMutex )
		{
			Job ff=waitingJobs[$-1];
			waitingJobs=waitingJobs.remove(waitingJobs.length-1);
			atomicOp!"+="(jobsDone, 1);
			return ff;
		}
	}
	void runNextJob(){
		Fiber fiber;
		Job job;
		synchronized( waitingFibersMutex )
		{
			if(waitingFibers.length>0){
				fiber=popFiber().fiber;
			}
		}
		synchronized( waitingJobsMutex )
		{
			if(fiber is null && waitingJobs.length>0){
				job=popJob();
				fiber= new Fiber( &job.run);
			}
		}
		if(fiber is null ){
			threadWorking[threadNum]=false;
			return;
		}
		threadWorking[threadNum]=true;
		assertLock(fiber.state==Fiber.State.HOLD);
		fiber.call();	
	}
	void threadRunFunction(){
		threadNum=Thread.getThis.name.to!uint;
		threadWorking[threadNum]=true;
		while(!exit){
			runNextJob();
		}
	}
}
struct FiberData{
	Fiber fiber;
	ThreadID pid;
}
FiberData getFiberData(){
	Fiber fiber=Fiber.getThis();
	assertLock(fiber !is null);
	return FiberData(fiber,Thread.getThis.id);
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








void async_sleep(uint mseconds){
	//Thread.sleep(1000.msecs);
	//Thread.sleep(mseconds.msecs);
	jobManager.addRunningFiberAndWait();
}


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
	foreach(i;0..1000){
		jobManager.addRunningFiberAndWait();
		assertLock(id==Thread.getThis.id);
	}
}
void randomRecursionJobs(){
	JobDelegate[] dels=makeTestJobsFrom(&simpleYield,100);
	jobManager.addJobAndWait(dels);
}
/// One Job and one Fiber.yield
void simpleYield(){
	jobManager.addRunningFiberAndWait();
}
void testPerformance(){		
	uint iterations=10;
	uint packetSize=10_000;
	StopWatch sw;
	sw.start();
	jobManager.resetCounters();
	writeln(jobManager.jobsAdded);
	writeln(jobManager.jobsDone);
	writeln(jobManager.fibersAdded);
	writeln(jobManager.fibersDone);
	JobDelegate[] dels=makeTestJobsFrom(&simpleYield,packetSize);
	foreach(i;0..iterations){
		jobManager.addJobAndWait(dels);
	}
	writeln(jobManager.jobsAdded);
	writeln(jobManager.jobsDone);
	writeln(jobManager.fibersAdded);
	writeln(jobManager.fibersDone);
	jobManager.print=true;
	assertLock(jobManager.jobsAdded==iterations*packetSize);
	assertLock(jobManager.jobsDone ==iterations*packetSize);
	assertLock(jobManager.fibersAdded==iterations*packetSize+iterations);
	assertLock(jobManager.fibersDone ==iterations*packetSize+iterations);
	sw.stop();  
	writefln( "Benchmark: %s*%s : %s[ms]",iterations,packetSize,sw.peek().msecs);
}
///main functionality test
unittest{
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
		jobManager.addJobAndWait((&randomRecursionJobs).toDelegate);
		writeln(jobManager.jobsAdded);
		writeln(jobManager.jobsDone);
		writeln(jobManager.fibersAdded);
		writeln(jobManager.fibersDone);
		assertLock(jobManager.jobsAdded==101);
		assertLock(jobManager.jobsDone==101);
		assertLock(jobManager.fibersAdded==102);
		assertLock(jobManager.fibersDone==102);
	}
	writeln("Start test");
	jobManager.init(16);
	jobManager.addJob((&startTest).toDelegate);
	jobManager.start();
	jobManager.waitForEnd();
	jobManager.end();
	writeln("End test");
}