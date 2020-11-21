package org.semanticweb.swse;

public abstract class MasterArgs<E extends MasterArgs<E>>{
	public static boolean DEFAULT_GZ_IN = true;
	public static boolean DEFAULT_GZ_OUT = true;
	
	public static final String GZ = ".gz";
	
	private String _logFile = null;
	
	public void setMasterLog(String logf){
		_logFile = logf;
	}
	
	public String getMasterLog(){
		return _logFile;
	}
	
	/**
	 * 
	 * @return The interface that need to be bound remotely to execute
	 * the task.
	 */
	public abstract RMIInterface getRMIInterface();
	
	/**
	 * 
	 * @return The task master that will coordinate and run the task.
	 */
	public abstract Master<E> getTaskMaster();
}
