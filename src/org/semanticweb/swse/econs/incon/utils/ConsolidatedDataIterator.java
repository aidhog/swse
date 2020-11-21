package org.semanticweb.swse.econs.incon.utils;

import java.util.HashSet;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.semanticweb.swse.econs.ercons.utils.ConsolidationIterator;
import org.semanticweb.yars.nx.Literal;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.Resource;

public class ConsolidatedDataIterator implements Iterator<Node[]> {
	Iterator<Node[]> _in;
	Iterator<Node[]> _currentIt = null;
	boolean _active = false;
	OverflowBuffer _buf;
	int[] _pos;

	int _skip = 0;
	int _skipL = 0;
	int _skipB = 0;
	int _keep = 0;

	Node[] _last = null;

	static HashSet<Node> BLACKLIST = new HashSet<Node>();
	static{
		BLACKLIST.add(new Resource("http://rdf.opiumfield.com/lastfm/friends/"));
		BLACKLIST.add(new Resource("http://rdf.opiumfield.com/lastfm/recentlovedtracks/"));
		BLACKLIST.add(new Resource("http://rdf.opiumfield.com/lastfm/recenttracks/"));
		BLACKLIST.add(new Resource("http://rdf.opiumfield.com/lastfm/neighbours/"));
	}

	public ConsolidatedDataIterator(Iterator<Node[]> in, int[] pos, String dir, int size){
		_buf = new OverflowBuffer(dir, size);
		_in = in;
		_pos = pos;
		loadNext();
	}

	private void loadNext(){
		if(_in.hasNext()) while(_in.hasNext()){
			Node[] next = _in.next();

			if(BLACKLIST.contains(next[0])){
				_skipB++;
				continue;
			}
			if(next[0] instanceof Literal){
				_skipL++;
				continue;
			}

			if(_last!=null && !_last[0].equals(next[0])){
				if(!_active){
					_skip++;
				}
				_currentIt = null;
				_active = false;
				_buf.clear();
			}

			_last = next;

			if(_active){
				return;
			}

			for(int p:_pos){
				if(!next[p].equals(ConsolidationIterator.NO_REWRITE)){
					_currentIt = _buf.iterator();
					_active = true;
					_keep++;
					return;
				}
			}

			_buf.processStatement(next);
		}

		_active = false;
		_last = null;
		_currentIt = null;
		_buf.clear();
	}

	public boolean hasNext() {
		return _active;
	}

	public Node[] next() {
		if(!hasNext()){
			throw new NoSuchElementException();
		}
		if(_currentIt!=null && _currentIt.hasNext()){
			return _currentIt.next();
		}

		Node[] next = _last;
		loadNext();
		return next;
	}

	public void remove() {
		throw new UnsupportedOperationException();
	}
	
	public int getSkippedEntities(){
		return _skip;
	}
	
	public int getSkippedLiterals(){
		return _skipL;
	}
	
	public int getSkippedBlacklisted(){
		return _skipB;
	}
	
	public int getKeptEntities(){
		return _keep;
	}
	
	public int getBufferedFiles(){
		return _buf._files;
	}
}
