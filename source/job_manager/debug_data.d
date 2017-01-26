module job_manager.debug_data;

//import std.datetime;
import core.time;
import job_manager.vector;
import job_manager.shared_vector;
import std.experimental.allocator;
import std.experimental.allocator.mallocator;

//Hepler types
alias ExecutionVector=Vector!Execution;
alias VectorOfExecutionVectors=LockedVector!ExecutionVector;
//static data
//StopWatch threadLocalWatch;
ExecutionVector threadLocalExecutions;
__gshared VectorOfExecutionVectors globalVectorOfExecutionVectors;

//local data initialization
static this(){
	//threadLocalWatch.start();
	threadLocalExecutions=Mallocator.instance.make!ExecutionVector;
	globalVectorOfExecutionVectors.add(threadLocalExecutions);
}
static ~this(){
	globalVectorOfExecutionVectors.removeElement(threadLocalExecutions);
	Mallocator.instance.dispose(threadLocalExecutions);
}

//shared data initialization
shared static this(){
	globalVectorOfExecutionVectors=Mallocator.instance.make!VectorOfExecutionVectors;
}
shared static ~this(){
	Mallocator.instance.dispose(globalVectorOfExecutionVectors);
}

//main functionality
void storeExecution(Execution exec){
	threadLocalExecutions~=exec;
}
//thread unsafe
void resetExecutions(){
	foreach(executions;globalVectorOfExecutionVectors[]){
		executions.reset();
	}
}
import std.algorithm:map,joiner;
//thread unsafe
auto getExecutions(){
	return globalVectorOfExecutionVectors[].map!((a) => a[]).joiner;
}


struct Execution{
	void* funcAddr;
	long startTime;
	long endTime;
	this(void* funcAddr){
		this.funcAddr=funcAddr;
		startTime=MonoTime.currTime.ticks;
	}
	void end(){
		endTime=MonoTime.currTime.ticks;
	}
	long dt(){
		return endTime-startTime;
	}
	long ticksPerSecond(){
		return MonoTime.ticksPerSecond;
	}
}