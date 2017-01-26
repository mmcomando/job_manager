module job_manager.debug_data;

import std.datetime;
import job_manager.vector;
import job_manager.shared_vector;
import std.experimental.allocator;
import std.experimental.allocator.mallocator;

alias DataVector=Vector!Execution;
StopWatch stopWatch;

DataVector vector;
static this(){
	stopWatch.start();
	vector=Mallocator.instance.make!DataVector;
	allData.add(vector);
}
static ~this(){
	allData.removeElement(vector);
	Mallocator.instance.dispose(vector);
}


alias DataDataVector=LockedVector!DataVector;
__gshared DataDataVector allData;
shared static this(){
	allData=Mallocator.instance.make!DataDataVector;
}
shared static ~this(){
	Mallocator.instance.dispose(allData);
}

void storeExecution(Execution exec){
	vector~=exec;
}
void resetExecutions(){
	DataVector arr;
	do{
		arr=allData.pop();
		if(arr is null){break;}
		arr.reset;
	}while(true);
}

auto getExecutions(){
	return allData;
}
struct Execution{
	void* funcAddr;
	TickDuration startTime;
	TickDuration endTime;
	this(void* funcAddr){
		this.funcAddr=funcAddr;
		startTime=stopWatch.peek();
	}
	void end(){
		endTime=stopWatch.peek();
	}
	TickDuration dt(){
		return endTime-startTime;
	}
}