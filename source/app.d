import std.stdio;
import job_manager;
import cache_vector;
import multithreaded_utils;
void main()
{
	version(unittest){}else{
		test(5);
		//testScalability();
		//testCV();
		//testAL();
	}
}
