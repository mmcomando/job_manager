module universal_delegate;
import std.traits:ReturnType,Parameters,isFunctionPointer,isDelegate,ParameterStorageClassTuple,ParameterStorageClass,AliasSeq;

template getPointer(T){
	alias getPointer = T*;	
}

///Replaces ref variables with pointer
private template getDelegateArgumentsSave(Delegate){
	alias getDelegateArgumentsSave=getDelegateArgumentsSaveImpl!(ParameterStorageClassTuple!Delegate,Parameters!Delegate).result;
}
private template getDelegateArgumentsSaveImpl(args...)
	if(args.length%2==0)
{
	enum half=args.length/2;
	alias pstc = args[0 .. half];
	alias tuple  = args[half .. $];
	
	static if (tuple.length)
	{
		alias head = tuple[0];
		alias tail = tuple[1 .. $];
		alias next = getDelegateArgumentsSaveImpl!(AliasSeq!(pstc[1..$],tuple[1..$])).result;
		static if (pstc[0] == ParameterStorageClass.ref_)
			alias result = AliasSeq!(getPointer!head, next);
		else
			alias result = AliasSeq!(head, next);
	}
	else
	{
		alias result = AliasSeq!();
	}
}



auto makeUniversalDelegate(T)(T del,Parameters!(T) args){
	static assert(Parameters!(T).length==args.length,"Parameters have to match" );
	return UniversalDelegate!(T)(del,args);
}

struct UniversalDelegate(Delegate)
{
	static assert(is(Delegate == function) || isFunctionPointer!Delegate || isDelegate!Delegate,"Provided type has to be: delegate, function, function pointer" );

	Delegate deleg;
	getDelegateArgumentsSave!Delegate argumentsSave;//for ref variables pointer is saved

	this(Delegate del,Parameters!Delegate args){
		static assert(Parameters!(Delegate).length==args.length,"Parameters have to match" );
		alias pstc=ParameterStorageClassTuple!Delegate;
		deleg=del;
		foreach(i,ref a;args){
			static if(pstc[i] == ParameterStorageClass.ref_){
				argumentsSave[i]=&a;
			}else{
				argumentsSave[i]=a;
			}
		}
	}
	ReturnType!Delegate call(){
		// Load arguments to orginal form
		Parameters!Delegate argumentsTmp;
		alias pstc=ParameterStorageClassTuple!Delegate;
		foreach(i,a;argumentsSave){
			static if(pstc[i] == ParameterStorageClass.ref_){
				argumentsTmp[i]=*a;
			}else{
				argumentsTmp[i]=a;
			}
		}
		// Call
		ReturnType!Delegate result=deleg(argumentsTmp);
		// Assign ref values to theirs orginal location
		foreach(i,a;argumentsSave){
			static if(pstc[i] == ParameterStorageClass.ref_){
				*a=argumentsTmp[i];
			}
		}
		return result;
	}
}

struct TestTmp{
	@nogc nothrow int add(int a,int b,ref ulong result) {
		result=a+b;
		return  a+b;
	}
}


@nogc nothrow unittest {
	TestTmp test;
	ulong returnByRef;
	auto universalDelegate=makeUniversalDelegate!(typeof(&test.add))(&test.add,2,2,returnByRef);
	auto result=universalDelegate.call();
	assert(result==4);
	assert(returnByRef==4);
}