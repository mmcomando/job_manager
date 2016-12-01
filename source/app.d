import std.stdio;
import job_manager;
import cache_vector;
void main()
{
	version(unittest){}else{
		test();
		testCV();
	}
}
