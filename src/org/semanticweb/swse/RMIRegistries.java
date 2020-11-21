package org.semanticweb.swse;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.Serializable;
import java.util.ArrayList;


/**
 * Stores and organises the details for multiple RMI registrys  
 * @author aidhog
 *
 */
public class RMIRegistries implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = -9009822107376448795L;
	private ArrayList<RMIRegistry> _list = new ArrayList<RMIRegistry>();
	private transient RMIRegistry _thisRegistry = null;
	private transient int _thisRegistryId = -1;

	public RMIRegistries(File servers, int defaultPort) throws Exception{
		BufferedReader br = new BufferedReader(new FileReader(servers));
		String line;
		while((line=br.readLine())!=null){
			line = line.trim();
			if(line.equals("") || line.startsWith("%")){
				continue;
			}

			int port = -1;
			String hostname = null;
			if(line.contains(":")){
				String[] sp = line.split(":");
				hostname = sp[0];
				port = Integer.parseInt(sp[1]);
			}
			else
				hostname = line;

			if(port!=-1)
				_list.add(new RMIRegistry(hostname, port));
			else
				_list.add(new RMIRegistry(hostname, defaultPort));
		}
	}

	public RMIRegistries(String[] servers, int defaultPort){
		for(String s:servers){
			s = s.trim();
			if(s.equals("") || s.startsWith("%")){
				continue;
			}

			int port = -1;
			String hostname = null;
			if(s.contains(":")){
				String[] sp = s.split(":");
				hostname = sp[0];
				port = Integer.parseInt(sp[1]);
			}
			else
				hostname = s;

			if(port!=-1)
				_list.add(new RMIRegistry(hostname, port));
			else
				_list.add(new RMIRegistry(hostname, defaultPort));
		}
	}

	public RMIRegistries(ArrayList<RMIRegistry> list) {
		_list = new ArrayList<RMIRegistry>();
		_list.addAll(list);

		System.out.println(_list);
	}

	public synchronized int getServerId(String serverURL, int port) {
		return _list.indexOf(new RMIRegistry(serverURL, port));
	}

	public synchronized int getServerNo(Object key) {
		int hash = Math.abs(key.hashCode());
		hash = Math.abs(hash);

//		int index = (int) Math.floor(hash * ((double)_list.size() / (double)Integer.MAX_VALUE));
		//in case of an unlikely rounding error
//		index = index % _list.size();
		
		hash = Math.abs(hash32shiftmult(hash));
//		int index = (int) ;
		return hash % _list.size();
	}

	public int hash32shiftmult(int key)
	{
		int c2=0x27d4eb2d; // a prime or an odd constant
		key = (key ^ 61) ^ (key >>> 16);
		key = key + (key << 3);
		key = key ^ (key >>> 4);
		key = key * c2;
		key = key ^ (key >>> 15);
		return key;
	}


	public void setThisServer(String server, int port){
		_thisRegistryId = getServerId(server, port);
		_thisRegistry = new RMIRegistry(server, port);
	}

	public void setThisServer(int serverNo){
		System.out.println("serverno" + serverNo);
		System.out.flush();
		_thisRegistryId = serverNo;
		_thisRegistry = _list.get(serverNo);
	}

	public RMIRegistry thisServer(){
		return _thisRegistry;
	}

	public int thisServerId(){
		return _thisRegistryId;
	}

	public ArrayList<RMIRegistry> getServerList() {
		return _list;
	}

	public RMIRegistry getServer(int n) {
		return _list.get(n);
	}

	public int getServerCount() {
		return _list.size();
	}

	public String toString(){
		if(_thisRegistry!=null)
			return _list.toString()+" this: "+_thisRegistry;
		return _list.toString();
	}
}
