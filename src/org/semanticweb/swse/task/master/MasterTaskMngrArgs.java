package org.semanticweb.swse.task.master;

import org.semanticweb.swse.MasterArgs;
import org.semanticweb.swse.task.RMITaskMngrServer;

public class MasterTaskMngrArgs extends MasterArgs<MasterTaskMngrArgs>{
	public static final RMITaskMngrServer DEFAULT_RI = new RMITaskMngrServer();
	public static final MasterTaskMngr DEFAULT_MASTER = new MasterTaskMngr();
	
	private final MasterArgs[] _tasks;
	
	public MasterTaskMngrArgs(MasterArgs... tasks){
		_tasks = tasks;
	}
	
	public MasterArgs[] getTasks(){
		return _tasks;
	}
	
	public String toString(){
		StringBuffer buf = new StringBuffer();
		for(int i=0; i<_tasks.length; i++){
			buf.append("\n=========TASK "+i+"=========\n");
			buf.append(_tasks[i]);
		}
		return buf.toString();
	}
	
	public RMITaskMngrServer getRMIInterface() {
		return DEFAULT_RI;
	}

	public MasterTaskMngr getTaskMaster() {
		return DEFAULT_MASTER;
	}
}