module job_manager.execution_manager;

import job_manager.vector;
import job_manager.shared_vector;

alias Delegate=void delegate();
class ExecutionManager{
	LockedVector!Delegate delegatesToExecute;

	void add(Delegate del){
		delegatesToExecute.add(del);
	}

	void update(){
		Vector!Delegate delegates=delegatesToExecute.vectorCopyWithReset();
		foreach(del;delegates){
			del();
		}
	}

}