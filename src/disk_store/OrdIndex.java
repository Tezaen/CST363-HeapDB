package disk_store;

import java.util.ArrayList;
import java.util.List;

/**
 * An ordered index.  Duplicate search key values are allowed,
 * but not duplicate index table entries.  In DB terminology, a
 * search key is not a superkey.
 * <p>
 * A limitation of this class is that only single integer search
 * keys are supported.
 */


public class OrdIndex implements DBIndex {

    private class Entry {
        int key;
        ArrayList<BlockCount> blocks;

        public String toString() {
            return key + " : " + blocks;
        }
    }

    private class BlockCount {
        int blockNo;
        int count;

        public String toString() {
            return "[" + blockNo + "," + count + "]";
        }
    }

    ArrayList<Entry> entries;

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
        if (entries.size() == 0) {
            return distinctBlockNums;
        }
        int l = 0;
        int r = entries.size() - 1;
        while (l <= r) {
            int m = l + (r - l) / 2;
            if (entries.get(m).key == key) {
                if (entries.get(m).blocks.size() > 0) {
                    List<BlockCount> tempBlock = entries.get(m).blocks;
                    for (BlockCount grab : entries.get(m).blocks) {
                    	if (!distinctBlockNums.contains(grab.blockNo)) {
							distinctBlockNums.add(grab.blockNo);
						}
					}
                    break;
                }
            } else if (key > entries.get(m).key) {
                l = m + 1;
            } else if (key < entries.get(m).key) {
                r = m - 1;
            }
        }
        return distinctBlockNums;
        //throw new UnsupportedOperationException();
    }

    @Override
    public void insert(int key, int blockNum) {
        //System.out.println("enter insert: "+entries); //debug
        //System.out.println("Enter lookup key: " + key + " blocknum: " + blockNum);
        int left = 0;
        int right = entries.size() - 1;
        int middle = 0;
        boolean found_key = false;
        while (left <= right) {

            middle = left + (right - left) / 2;

            if (entries.get(middle).key == key) {
                found_key = true;
                break;
            } else if (entries.get(middle).key < key) {
                left = middle + 1;
            } else {
                right = middle - 1;
            }
        }

        if (found_key) {
            int theBlockNo;
            boolean foundBlock = false;
            for (BlockCount b : entries.get(middle).blocks) {
                if (b.blockNo == blockNum) {
                    b.count++;
                    foundBlock = true;
                    break;
                }
            }
            if (!foundBlock) {
                BlockCount tempBlock = new BlockCount();
                tempBlock.blockNo = blockNum;
                tempBlock.count = 1;
                entries.get(middle).blocks.add(tempBlock);
            }
        } else {
            Entry newEntry = new Entry();
            BlockCount newBlockCount = new BlockCount();
            ArrayList<BlockCount> newBlockCountlist = new ArrayList<>();
            newEntry.key = key;
            newEntry.blocks = newBlockCountlist;
            newBlockCount.blockNo = blockNum;
            newBlockCount.count = 1;
            newEntry.blocks.add(newBlockCount);
            entries.add(newEntry);
        }
        //throw new UnsupportedOperationException();
    }

    @Override
    public void delete(int key, int blockNum) {
        // lookup key
        //  if key not found, should not occur.  Ignore it.
        //  decrement count for blockNum.
        //  if count is now 0, remove the blockNum.
        //  if there are no block number for this key, remove the key entry.
        //throw new UnsupportedOperationException();

        int left = 0;
        int right = entries.size() - 1;
        boolean foundBlock = false;
        while (left <= right) {
			int middle = left + (right - left) / 2;
            if (entries.get(middle).key == key) {
            	boolean deleteBlock = false;
            	List<BlockCount> foundBlockList = new ArrayList<>();
                for (BlockCount b : entries.get(middle).blocks) {
                    if (b.blockNo == blockNum) {
                        foundBlock = true;
                        b.count--;
                    }
                    if (b.count == 0) {
                    	deleteBlock = true;
						foundBlockList.add(b);
					}
                }
                if (deleteBlock) {
                	entries.get(middle).blocks.removeAll(foundBlockList);
				}
				if (entries.get(middle).blocks.size() == 0) {
					entries.remove(entries.get(middle));
				}
                if (foundBlock) {
                    return;
                }
            } else if (entries.get(middle).key < key) {
                left = middle + 1;
            } else {
                right = middle - 1;
            }
        }
    }

    /**
     * Return the number of entries in the index
     *
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