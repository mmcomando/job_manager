module job_manager.universal_delegate;
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
	return UniversalDelegate!(T)(del,args);
}

struct UniversalDelegate(Delegate)
{
	static assert(is(Delegate == function) || isFunctionPointer!Delegate || isDelegate!Delegate,"Provided type has to be: delegate, function, function pointer" );
	enum hasReturn=!is(ReturnType!Delegate==void);
	Delegate deleg;
	getDelegateArgumentsSave!Delegate argumentsSave;//for ref variables pointer is saved
	static if(hasReturn)ReturnType!Delegate result;

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
		static if(hasReturn){
			ReturnType!Delegate result=deleg(argumentsTmp);
		}else{
			deleg(argumentsTmp);
		}
		// Assign ref values to theirs orginal location
		foreach(i,a;argumentsSave){
			static if(pstc[i] == ParameterStorageClass.ref_){
				*a=argumentsTmp[i];
			}
		}
		static if(hasReturn)return result;
	}
	void callAndSaveReturn(){
		static if(hasReturn){
			result=call();
		}else{
			call();
		}
	}
}


@nogc nothrow:
/// Using Deleagte
unittest {
	static struct TestTmp{
		@nogc nothrow int add(int a,int b,ref ulong result) {
			result=a+b;
			return  a+b;
		}
	}
	TestTmp test;
	ulong returnByRef;
	auto universalDelegate=makeUniversalDelegate!(typeof(&test.add))(&test.add,2,2,returnByRef);
	auto result=universalDelegate.call();
	assert(result==4);
	assert(returnByRef==4);

}

/// Using Function
unittest {
	@nogc nothrow int add(int a,int b,ref ulong result) {
		result=a+b;
		return  a+b;
	}
	ulong returnByRef;
	auto universalDelegate=makeUniversalDelegate!(typeof(&add))(&add,2,2,returnByRef);
	auto result=universalDelegate.call();
	assert(result==4);
	assert(returnByRef==4);
}
// void with no parameters
unittest {
	static int someNum;
	static @nogc nothrow void add() {
		someNum=200;
	}
	auto universalDelegate=makeUniversalDelegate!(typeof(&add))(&add);
	universalDelegate.call();
	assert(someNum==200);
}