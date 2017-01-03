﻿module job_manager.shared_utils;

import core.thread;
import core.cpuid:threadsPerCPU;
import core.stdc.string:memset,memcpy;
import core.stdc.stdlib:malloc,free;
import std.stdio:writeln,writefln;
import std.random:uniform;
import std.conv:to;
import std.experimental.allocator.building_blocks;


public import std.experimental.allocator:make,makeArray,dispose;

shared Mallocator mallocator;
//shared GCAllocator mallocator;
shared static this(){
	mallocator=Mallocator.instance;
}




//useful for testing if function is safe in multthreated enviroment
//name can be used as id
void testMultithreaded(void delegate() func,uint threadsCount=0){
	if(threadsCount==0)
		threadsCount=threadsPerCPU;
	Thread[] threadPool=mallocator.makeArray!(Thread)(threadsCount);
	foreach(i;0..threadsCount){
		Thread th=mallocator.make!Thread(func);
		th.name=i.to!string;//maybe there is better way to pass data to a thread?
		threadPool[i]=th;
	}
	foreach(thread;threadPool)thread.start();
	foreach(thread;threadPool)thread.join();
	foreach(thread;threadPool)mallocator.dispose(thread);
	mallocator.dispose(threadPool);	
}
