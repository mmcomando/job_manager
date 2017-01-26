/**
Module which presents functionality to the user
 */
module job_manager;

private import job_manager.manager;
public import job_manager.manager_utils:multithreated;
private import job_manager.manager_tests;

void startMainLoop(void function() mainLoop,uint threadsCount=0){
	jobManager.startMainLoop(mainLoop,threadsCount);
}
void testMultithreated(){
	testScalability();
}