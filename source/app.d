import std.stdio:writeln;

public import job_manager;

void my_main()
{
	int[] ints;
	ints.length=200;
	shared uint sum=0;
	foreach(ref int el;ints.multithreated){
		import core.atomic;
		atomicOp!"+="(sum,1);
	}
}
void main()
{
	jobManager.startMainLoop(&my_main);
}
