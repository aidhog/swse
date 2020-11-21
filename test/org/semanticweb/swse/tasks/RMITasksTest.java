package org.semanticweb.swse.tasks;

import org.semanticweb.swse.MasterArgs;
import org.semanticweb.swse.tasks.Tasks;

import junit.framework.TestCase;

public class RMITasksTest extends TestCase{
	
	private static final String IN = "testdata/out%/data.nq";
	private static final boolean GZ_IN = false;
	
	private static final String RREDIRS = "testdata/out%/redirs.nx";
	private static final boolean GZ_RRED = false;
	
	private static final String LREDIRS = "testdata/out%/redirs.nx";
	private static final boolean GZ_LRED = false;
	
	private static final String OUT = "testdata/swse/";
	
	public static void main(String[] args) throws Exception{
		MasterArgs[] mas = Tasks.createTask(IN, GZ_IN, LREDIRS, GZ_LRED, RREDIRS, GZ_RRED, OUT);
		
		for(MasterArgs ma:mas){
			System.err.println(ma);
		}
	}
	
}
