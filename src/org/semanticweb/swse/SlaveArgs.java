package org.semanticweb.swse;

import java.io.Serializable;

public abstract class SlaveArgs implements Serializable{
	protected String _logFile = null;
	
	public void setSlaveLog(String logf){
		_logFile = logf;
	}
	
	public String getSlaveLog(){
		return _logFile;
	}
	
	public abstract SlaveArgs instantiate(int server);
}
