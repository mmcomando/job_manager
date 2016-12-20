import std.stdio;
import job_manager;
import cache_vector;
import multithreaded_utils;
void main()
{
	import core.memory;
	//GC.disable();
	version(DigitalMars){
		import etc.linux.memoryerror;
		registerMemoryErrorHandler();
	}
	version(unittest){}else{
		foreach(i;0..10)
		testScalability();
		//testScalability();
		//testCV();
		//testAL();
	}
}
