import std.stdio:writeln;

import job_manager;

void my_main()
{
	int[200] ints;
	shared uint sum=0;
	foreach(ref int el;ints.multithreated){
		import core.atomic;
		atomicOp!"+="(sum,1);
	}
}
void main()
{
	while(1)
	testMultithreated();
	//jobManager.startMainLoop(&my_main);
}
