package disk_store;

import java.util.ArrayList;
import java.util.List;

/**
 * An ordered index.  Duplicate search key values are allowed,
 * but not duplicate index table entries.  In DB terminology, a
 * search key is not a superkey.
 * 
 * A limitation of this class is that only single integer search
 * keys are supported.
 *
 */

public class OrdIndex implements DBIndex {
	
	private class Entry {
		int key;
		ArrayList<BlockCount> blocks;
	}
	
	private class BlockCount {
		int blockNo;
		int count;
	}
	
	ArrayList<Entry> entries;
	int size = 0;
	
	/**
	 * Create an new ordered index.
	 */
	public OrdIndex() {
		entries = new ArrayList<>();
	}
	
	@Override
	public List<Integer> lookup(int key) {
		// binary search of entries arraylist
		// return list of block numbers (no duplicates). 
		// if key not found, return empty list
		List<Integer> distinctBlockNums = new ArrayList<>();
		int l;
		int r = size();
		for(l = 0; r - l > 1;){
			int m = (r + l) / 2;
			if(entries.get(m).key == key){
				for(BlockCount grab : entries.get(m).blocks){
					distinctBlockNums.add(grab.blockNo);
				}
			}else if(key > entries.get(m).key){
				l = m;
			}else if(key < entries.get(m).key){
				r = m;
			}
		}
		return distinctBlockNums;

		//throw new UnsupportedOperationException();
	}
	
	@Override
	public void insert(int key, int blockNum) {
		if(this.lookup(key) != null) {
			return;
		} else {
			Entry newEntry = new Entry();
			BlockCount newBlockCount = new BlockCount();
			newEntry.key = key;
			newBlockCount.blockNo = blockNum;
			newBlockCount.count = 0; //What is this?
			newEntry.blocks.add(newBlockCount);
		}
		//throw new UnsupportedOperationException();
	}

	@Override
	public void delete(int key, int blockNum) {
//		int count = size();
//		if(this.lookup(key) == null) {
//			count--;
//		}if(count == 0){
//			//remove blockNum
//		}
		// lookup key 
		//  if key not found, should not occur.  Ignore it.
		//  decrement count for blockNum.
		//  if count is now 0, remove the blockNum.
		//  if there are no block number for this key, remove the key entry.
		throw new UnsupportedOperationException();
	}
	
	/**
	 * Return the number of entries in the index
	 * @return
	 */
	public int size() {
		return entries.size();
	}
	
	@Override
	public String toString() {
		throw new UnsupportedOperationException();
	}
}