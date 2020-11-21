package org.semanticweb.swse.file;

import org.semanticweb.swse.RMIUtils;
import org.semanticweb.swse.SlaveArgs;

public class SlaveFileArgs extends SlaveArgs {
	/**
	 * 
	 */
	private static final long serialVersionUID = 2366745716801778239L;
	
	
	public SlaveFileArgs(){
		;
	}
	
	public SlaveFileArgs instantiate(int server) {
		SlaveFileArgs sla = new SlaveFileArgs();
		sla.setSlaveLog(RMIUtils.getLocalName(_logFile, server));
		return sla;
	}
	
	public String toString(){
		return "slaveFileServer (no args)";
	}	
}