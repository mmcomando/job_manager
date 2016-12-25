import std.stdio;
import job_manager;
import cache_vector;
import multithreaded_utils;
import job_vector;
void main()
{
	import core.memory;
	//GC.disable();
	version(DigitalMars){
		import etc.linux.memoryerror;
		registerMemoryErrorHandler();
	}
	version(unittest){}else{
		//foreach(i;0..10000)testLLQ();
		//
		foreach(i;0..100)testScalability();
		//testCV();
		//testAL();
	}
}
