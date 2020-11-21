package org.semanticweb.swse;

public interface Master<E extends MasterArgs>  {
	public void startRemoteTask(RMIRegistries servers, String stubName, E args) throws Exception;
}
