module job_manager.utils;

import std.traits;
import std.stdio:writefln,writeln;

// Casts @nogc out of a function or delegate type.
auto assumeNoGC(T) (T t) if (isFunctionPointer!T || isDelegate!T)
{
	enum attrs = functionAttributes!T | FunctionAttribute.nogc;
	return cast(SetFunctionAttributes!(T, functionLinkage!T, attrs)) t;
}

void writelnng(T...)(T args){
	assumeNoGC( (T arg){writeln(arg);})(args);
}




version(linux){
	import std.conv;
	import std.demangle;
	private static struct  Dl_info {
		const char *dli_fname; 
		void       *dli_fbase;  
		const char *dli_sname;  
		void       *dli_saddr; 
	}
	private extern(C) int dladdr(void *addr, Dl_info *info);
	
	string functionName(void* addr){
		Dl_info info;
		int ret=dladdr(addr,&info);
		return info.dli_sname.to!(string).demangle;
	}
}else{
	string functionName(void* addr){
		return "?? Use Linux";
	}
}

void printException(Exception e, int maxStack = 4) {
	writeln("Exception message: ", e.msg);
	writefln("File: %s Line Number: %s Thread: %s", e.file, e.line);
	writeln("Call stack:");
	foreach (i, b; e.info) {
		writeln(b);
		if (i >= maxStack)
			break;
	}
	writeln("--------------");
}
void printStack(){
	static immutable Exception exc=new Exception("Dummy");
	try{
		throw exc;
	}catch(Exception e ){
		printException(e);
	}
}