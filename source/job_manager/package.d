module job_manager;

public import job_manager.manager:jobManager;
public import job_manager.manager;
public import job_manager.manager_utils:multithreated;
public import job_manager.manager_tests;



void testMultithreated(){
	testScalability();
}