module job_manager.manager;

static if(1){
	public import job_manager.manager_multithreated;
}else{
	public import job_manager.manager_singlethreated;
}
public import job_manager.manager_utils; 
public import job_manager.universal_delegate; 