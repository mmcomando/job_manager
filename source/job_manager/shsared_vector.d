module job_manager.shared_vector;


import std.experimental.allocator;
import std.experimental.allocator.mallocator;

import job_manager.shared_allocator;
import job_manager.vector;





////////////////////
import std.algorithm:remove;

class LockedVectorBuildIn(T){
	T[] array;
public:
	bool empty(){
		return(array.length==0);
	}	
	
	void add( T  t ) {
		synchronized( this ){
			array.assumeSafeAppend~=t;
		}
	}	
	void add( T[]  t ) {
		synchronized( this ){
			array.assumeSafeAppend~=t;
		}
	}
	
	T pop(  ) {
		synchronized( this ){
			if(array.length==0)return T.init;
			T obj=array[$-1];
			array=array.remove(array.length-1);
			return obj;
		}
	}
	
}

class LockedVector(T){
	Vector!T array;
public:
	this(){
		array=Mallocator.instance.make!(Vector!T);
	}
	~this(){
		Mallocator.instance.dispose(array);
	}
	bool empty(){
		return(array.length==0);
	}	
	
	void add( T  t ) {
		synchronized( this ){
			array~=t;
		}
	}	
	void add( T[]  t ) {
		synchronized( this ){
			array~=t;
		}
	}
	void removeElement( T elem ) {
		synchronized( this ){
			array.removeElement(elem);
		}
	}
	
	T pop(  ) {
		synchronized( this ){
			if(array.length==0)return T.init;
			T obj=array[$-1];
			array.remove(array.length-1);
			return obj;
		}
	}
	auto opSlice(){
		return array[];
	}
	
}