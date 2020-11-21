package org.semanticweb.swse.econs.incon.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.parser.Callback;
import org.semanticweb.yars.nx.parser.NxParser;
import org.semanticweb.yars.util.CallbackNxOutputStream;
import org.semanticweb.yars.util.FlyweightNodeIterator;
import org.semanticweb.yars.util.PleaseCloseTheDoorWhenYouLeaveIterator;

public class OverflowBuffer implements Callback {
	private static final String TMP_FILE_SUFFIX = ".tmp.nx.gz";
	private static final int FLYWEIGHT = 5000;
	
	ArrayList<Node[]> _inmem;
	String _dir;
	int _files = 0;
	
	CallbackNxOutputStream _cfile = null;
	OutputStream _cos = null;
	String _cfn = null;
	
	int _max = Integer.MAX_VALUE;
	
	boolean _disk = false;
	
	public OverflowBuffer(String tmpdir, int size){
		_inmem = new ArrayList<Node[]>(size+1);
		File f = new File(tmpdir);
		f.mkdirs();
		_dir = tmpdir;
		_max = size;
	}
	
	public void endDocument() {
		;
	}

	public void processStatement(Node[] nx) {
		if(_disk){
			_cfile.processStatement(nx);
		} else{
			_inmem.add(nx);
			if(_inmem.size()==_max){
				try{
					_cfn = _dir+"/"+_files+TMP_FILE_SUFFIX;
					_files++;
					File tmp = new File(_cfn);
					tmp.deleteOnExit();
					_cos = new GZIPOutputStream(new FileOutputStream(tmp));
					_cfile = new CallbackNxOutputStream(_cos);
					_disk = true;
				} catch(Exception e){
					throw new RuntimeException(e);
				}
				
				for(Node[] na:_inmem){
					_cfile.processStatement(na);
				}
				_inmem.clear();
			}
		}
	}

	public void startDocument() {
		;
	}
	
	public void clear() {
		if(_disk){
			_disk = false;
			try {
				_cos.close();
			} catch (IOException e) {
				;
			}
		}
		_inmem.clear();
	}

	public Iterator<Node[]> iterator() {
		if(_disk){
			try {
				_cos.close();
				InputStream fis = new GZIPInputStream(new FileInputStream(_cfn));
				NxParser nxp = new NxParser(fis);
				Iterator<Node[]> fly = new FlyweightNodeIterator(FLYWEIGHT, nxp);
				return new PleaseCloseTheDoorWhenYouLeaveIterator(fly, fis);
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		} else{
			return _inmem.iterator();
		}
	}
}
