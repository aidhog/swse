package org.semanticweb.swse;

import java.io.File;
import java.io.IOException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.util.ArrayList;
import java.util.Collections;
import java.util.logging.FileHandler;
import java.util.logging.Handler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

public class RMIUtils {
	static{
		System.setProperty("sun.rmi.transport.tcp.handshakeTimeout", Integer.toString(5*60*1000));
		System.setProperty("sun.rmi.transport.connectionTimeout", Integer.toString(60*1000));
	}
	
	public static String getLocalName(String name, int server){
		if(name==null)
			return name;
		
		return name.replaceAll("%", Integer.toString(server));
	}
	
	public static String[] getLocalNames(String[] names){
		if(names==null)
			return names;
		
		String[] localnames = new String[names.length];
		
		for(int i=0; i<names.length; i++){
			localnames[i] = getLocalName(names[i]);
		}
		
		return localnames;
	}
	
	public static String[] getLocalNames(String[] names, int server){
		if(names==null)
			return names;
		
		String[] localnames = new String[names.length];
		
		for(int i=0; i<names.length; i++){
			localnames[i] = getLocalName(names[i], server);
		}
		
		return localnames;
	}
	
	public static String getLocalName(String name){
		if(name==null)
			return name;
		
		return name.replaceAll("%", "");
	}
	
	public static String[] getLocalNames(String name, int servers){
		if(name==null)
			return null;
		
		String[] names = new String[servers];
		for(int i=0; i<servers; i++){
			names[i] = name.replaceAll("%", Integer.toString(i));
		}
		return names;
	}
	
	public static void startRMIRegistry(int port) throws RemoteException{
		LocateRegistry.createRegistry(port);
	}
	
	public static boolean mkdirs(String dir){
		File f = new File(dir);
		return f.mkdirs();
	}
	
	public static boolean mkdirsForFile(String file){
		File f = new File(file);
		return f.getParentFile().mkdirs();
	}
	
	public static void setLogFile(String file) throws SecurityException, IOException{
		if(file!=null){
			System.err.println("Setting logger to "+file);
			mkdirsForFile(file);
		
			Logger root = Logger.getLogger("");
			if(root.getHandlers()!=null) for(Handler h:root.getHandlers())
				root.removeHandler(h);
			FileHandler fh = new FileHandler(file);
			fh.setFormatter(new SimpleFormatter());
			
			root.addHandler(fh);
		}
	}
	
	public static void startRandomOrder(Thread[] ts){
		ArrayList<Thread> els = new ArrayList<Thread>();
		for(Thread t:ts)
			els.add(t);
		
		Collections.shuffle(els);
		
		for(Thread t:els)
			t.start();
	}
}
