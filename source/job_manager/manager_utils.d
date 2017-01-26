module job_manager.manager_utils;

import core.atomic;
import std.format:format;

import job_manager.manager;

import job_manager.fiber_cache;
import job_manager.shared_utils;
import job_manager.shared_queue;
import job_manager.utils;
import job_manager.universal_delegate;
import job_manager.debug_data;
import job_manager.debug_sink;

import std.traits:Parameters;
import core.thread:Thread,ThreadID,sleep,Fiber;

uint jobManagerThreadNum;//thread local var
alias JobDelegate=void delegate();
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
		assert(atomicLoad(count)<9000);
		assert(waitingFiber.fiber !is null);
		
		atomicOp!"-="(count, 1);
		bool ok=cas(&count,0,10000);
		if(ok){
			jobManager.addFiber(waitingFiber);
			//waitingFiber.fiber=null;//makes deadlock maybe atomicStore would help or it shows some bug??
			//atomicStore(waitingFiber.fiber,null);//has to be shared ignore for now
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
		//Execution exec=Execution(unDel.getFuncPtr);
		unDel.callAndSaveReturn();
		//exec.end();
		//storeExecution(exec);
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
	jobManager.addJobAndYield(unJob.delPointer);
	static if(unJob.unDel.hasReturn){
		return unJob.unDel.result;
	}
}

auto multithreated(T)(T[] slice){
	
	static struct Tmp {
		import std.traits:ParameterTypeTuple;
		T[] array;
		int opApply(Dg)(scope Dg dg)
		{ 
			static assert (ParameterTypeTuple!Dg.length == 1 || ParameterTypeTuple!Dg.length == 2);
			enum hasI=ParameterTypeTuple!Dg.length == 2;
			static if(hasI)alias IType=ParameterTypeTuple!Dg[0];
			static struct NoGcDelegateHelper{
				Dg del;
				T[] arr;
				static if(hasI)IType iStart;
				
				void call() { 
					foreach(int i,ref element;arr){
						static if(hasI){
							IType iSend=iStart+i;
							int result=del(iSend,element);
						}else{
							int result=del(element);
						}
						assert(result==0,"Cant use break, continue, itp in multithreated foreach");
					}	
				}
			}
			enum partsNum=16;//constatnt number == easy usage of stack
			if(array.length<partsNum){
				foreach(int i,ref element;array){
					static if(hasI){
						int result=dg(i,element);
					}else{
						int result=dg(element);
					}
					assert(result==0,"Cant use break, continue, itp in multithreated foreach");
					
				}
			}else{
				NoGcDelegateHelper[partsNum] helpers;
				uint step=cast(uint)array.length/partsNum;
				
				alias ddd=void delegate();
				UniversalJobGroup!ddd group=UniversalJobGroup!ddd(partsNum);
				mixin(getStackMemory("group"));
				foreach(int i;0..partsNum-1){
					helpers[i].del=dg;
					helpers[i].arr=array[i*step..(i+1)*step];
					static if(hasI)helpers[i].iStart=i*step;
					group.add(&helpers[i].call);
				}
				helpers[partsNum-1].del=dg;
				helpers[partsNum-1].arr=array[(partsNum-1)*step..array.length];
				static if(hasI)helpers[partsNum-1].iStart=(partsNum-1)*step;
				group.add(&helpers[partsNum-1].call);
				
				group.wait();
			}
			return 0;
			
		}
	}
	Tmp tmp;
	tmp.array=slice;
	return tmp;
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
	//but remember: that data is stack allocated
	auto wait(){
		assert(jobsAdded==jobsNum);
		counter.count=jobsNum;
		counter.waitingFiber=getFiberData();
		foreach(ref unDel;unJobs){
			unDel.counter=&counter;
		}
		//writeln("+44");
		jobManager.addJobsAndYield(dels);
		//writeln("+55");
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
	import std.experimental.allocator;
	import std.experimental.allocator.mallocator;
    import core.stdc.string:memset;
	uint ___jobsNum%s=%s.jobsNum;
	
	%s.unJobs=Mallocator.instance.makeArray!(%s.UnJob)(___jobsNum%s);
	%s.dels=Mallocator.instance.makeArray!(JobDelegate*)(___jobsNum%s);

	

    scope(exit){
      /* memset(%s.unJobs.ptr,0,%s.UnJob.sizeof*___jobsNum%s);
       memset(%s.dels.ptr,0,(JobDelegate*).sizeof*___jobsNum%s);
	   Mallocator.instance.dispose(%s.unJobs);
	   Mallocator.instance.dispose(%s.dels);*/
	}


		",varName,varName,varName,varName,
		varName,varName,varName,varName,
		varName,varName,varName,varName,
		varName,varName);
	return code;
	
	
}