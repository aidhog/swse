package org.semanticweb.swse;

import java.io.Serializable;

/**
 * Representing a hostname and port for locating an RMI registry.
 * @author aidhog
 *
 */
public class RMIRegistry implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 6748321611032640947L;
	private int _port;
	private String _server;
	
	public RMIRegistry(String server, int port){
		_server = server;
		_port = port;
	}
	
	public String getServerUrl(){
		return _server;
	}
	
	public int getPort(){
		return _port;
	}
	
	public String toString(){
		return _server+":"+_port;
	}
}
