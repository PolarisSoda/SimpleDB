package simpledb.tx.concurrency;

import java.util.*;

import org.apache.derby.diag.LockTable;

import simpledb.file.BlockId;

/**
 * The lock table, which provides methods to lock and unlock blocks.
 * If a transaction requests a lock that causes a conflict with an
 * existing lock, then that transaction is placed on a wait list.
 * There is only one wait list for all blocks.
 * When the last lock on a block is unlocked, then all transactions
 * are removed from the wait list and rescheduled.
 * If one of those transactions discovers that the lock it is waiting for
 * is still locked, it will place itself back on the wait list.
 * @author Edward Sciore
 */

/*
SLock,XLock,UnLock이 transaction number도 추가로 입력받게 설정한다.

lock변수는 변경되어야 한다. 
-> block과 block위에서 lock을 쥐고 있는 transaction id의 list를 맵하도록.
The variable locks must be changed so that a block maps to a list of the transaction ids that hold a lock on the block 
(instead of just an integer). Use a negative transaction id to denote an exclusive lock.

Each time through the while loop in sLock and xLock, 
check to see if the thread needs to be aborted
(that is, if there is a transaction on the list that is older than the current transaction).
If so, then the code should throw a LockAbortException.

You will also need to make trivial modifications to the classes Transaction and ConcurrencyMgr
so that the transaction id gets passed to the lock manager methods.
 */

class LockTable {
   private static final long MAX_TIME = 10000; // 10 seconds
   //private Map<BlockId,Integer> locks = new HashMap<BlockId,Integer>();
   private Map<BlockId,ArrayList<Integer>> locks = new HashMap<BlockId,ArrayList<Integer>>();
   
   /**
    * Grant an SLock on the specified block.
    * If an XLock exists when the method is called,
    * then the calling thread will be placed on a wait list
    * until the lock is released.
    * If the thread remains on the wait list for a certain 
    * amount of time (currently 10 seconds),
    * then an exception is thrown.
    * @param blk a reference to the disk block
    */
   public synchronized void sLock(BlockId blk,int txnum) {
      try {
         int having_tx;
         while((having_tx = hasXlock(blk)) != 0) {
            //이 block에 대해 Xlock를 가지고 있는 친구가 있다면
            if(having_tx < txnum) throw new LockAbortException(); //근데 이 친구가 저보다 오래됐네요?
            else wait(); //젊으면 기다리기.
         }
         ArrayList<Integer> lock_list = locks.containsKey(blk) ? locks.get(blk) : new ArrayList<>();
         lock_list.add(txnum);
         locks.put(blk,lock_list);
      } catch(InterruptedException e) {
         throw new LockAbortException();
      }
   }
   
   /**
    * Grant an XLock on the specified block.
    * If a lock of any type exists when the method is called,
    * then the calling thread will be placed on a wait list
    * until the locks are released.
    * If the thread remains on the wait list for a certain 
    * amount of time (currently 10 seconds),
    * then an exception is thrown.
    * @param blk a reference to the disk block
    */
    /*
     * 실제로 xlock를 호출하는 경우엔, slock를 먼저 obtain하고 xlock를 얻어가기 때문에
     * 다른 코드가 필요없는 것으로 보인다.
     */
   synchronized void xLock(BlockId blk,int txnum) {
      try {
         while(hasOtherSLocks(blk,txnum) == true) {
            Integer having_tx = getOldestLock(blk,txnum);
            if(having_tx < txnum) throw new LockAbortException();
            else wait();
         }
         ArrayList<Integer> lock_list = locks.containsKey(blk) ? locks.get(blk) : new ArrayList<>();
         lock_list.remove(Integer.valueOf(txnum));
         lock_list.add(txnum * -1);
         locks.put(blk,lock_list);
      } catch(InterruptedException e) {
         throw new LockAbortException();
      }
   }
   
   /**
    * Release a lock on the specified block.
    * If this lock is the last lock on that block,
    * then the waiting transactions are notified.
    * @param blk a reference to the disk block
    */
   synchronized void unlock(BlockId blk,int txnum) {
      ArrayList<Integer> lock_list = locks.get(blk);
      if(lock_list != null) {
         for(int num : lock_list) {
            if(txnum == Math.abs(num)) lock_list.remove(Integer.valueOf(num));
         }
         if(lock_list.size() == 0) {
            locks.remove(blk);
            notifyAll();
         } else {
            locks.put(blk,lock_list);
         }
      }
   }
   
   private int hasXlock(BlockId blk) {
      ArrayList<Integer> lock_list = locks.get(blk);
      if(lock_list == null || lock_list.size() == 0) return 0;
      for(Integer id : lock_list) if(id < 0) return id.intValue();
      return 0;
   }
   
   private boolean hasOtherSLocks(BlockId blk,int txnum) {
      //만약 Array안에 오직 하나만 있고 그게 txnum이라면.....
      ArrayList<Integer> lock_list = locks.get(blk);
      if(lock_list == null) return true;
      if(lock_list != null && lock_list.size() == 1 && lock_list.contains(txnum)) return false;
      return false;
   }
   
   private int getOldestLock(BlockId blk,int txnum) {
      int ret = 0x7FFFFFFF;
      ArrayList<Integer> lock_list = locks.get(blk);
      if(lock_list != null)
         for(int num : lock_list) if(ret > Math.abs(num)) ret = Math.abs(num);
      return ret;
   }
   
}
