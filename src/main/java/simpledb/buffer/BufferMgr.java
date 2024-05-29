package simpledb.buffer;

import simpledb.file.*;
import simpledb.log.LogMgr;
import java.util.LinkedList;
import java.util.Map;
import java.util.HashMap;

public class BufferMgr { 
   private LinkedList<Buffer> unpinned_list;
   private Map<BlockId,Buffer> map_buffer;
   private int numAvailable;   /* the number of available (unpinned) buffer slots */
   private static final long MAX_TIME = 10000; /* 10 seconds */
   
   public BufferMgr(FileMgr fm, LogMgr lm, int numbuffs) {
      this.unpinned_list = new LinkedList<>();
      this.map_buffer = new HashMap<>();
      this.numAvailable = numbuffs;
      for(int i=0; i<numbuffs; i++) unpinned_list.add(new Buffer(fm,lm,i));
   }

   public synchronized int available() {
      return numAvailable;
   }
   
   public synchronized void flushAll(int txnum) {
      for(Buffer buff : map_buffer.values())
         if(buff.modifyingTx() == txnum) buff.flush();
   }
   
   public synchronized void unpin(Buffer buff) {
      buff.unpin();
      if (!buff.isPinned()) {
         unpinned_list.add(buff);
         numAvailable++;
         notifyAll();
      }
   }
   
   public synchronized Buffer pin(BlockId blk) {
      try {
         long timestamp = System.currentTimeMillis();
         Buffer buff = tryToPin(blk);
         while (buff == null && !waitingTooLong(timestamp)) {
            wait(MAX_TIME);
            buff = tryToPin(blk);
         }
         if (buff == null)
            throw new BufferAbortException();
         return buff;
      }
      catch(InterruptedException e) {
         throw new BufferAbortException();
      }
   }  
   
   public void printStatus() {
      for(Map.Entry<BlockId,Buffer> entry : map_buffer.entrySet()) {
         BlockId now_block = entry.getKey();
         Buffer now_buff = entry.getValue();
         String str = now_buff.isPinned() ? "pinned" : "unpinned";
         System.out.printf("Buffer %d: %s %s\n",now_buff.getID(),now_block.toString(),str);
      }
      System.out.print("Unpinned Buffers in LRU order:");
      for(Buffer buff : unpinned_list) System.out.printf(" %d",buff.getID());
   }

   private boolean waitingTooLong(long starttime) {
      return System.currentTimeMillis() - starttime > MAX_TIME;
   }

   private Buffer tryToPin(BlockId blk) {
      Buffer buff = findExistingBuffer(blk);
      if(buff == null) {
         buff = chooseUnpinnedBuffer();
         if(buff == null) return null;
         if(buff.block() != null) map_buffer.remove(buff.block());
         buff.assignToBlock(blk);
         map_buffer.put(blk,buff);
      }
      if(!buff.isPinned())
         numAvailable--;
      buff.pin();
      return buff;
   }
   
   private Buffer findExistingBuffer(BlockId blk) {
      return map_buffer.get(blk);
   }
   
   private Buffer chooseUnpinnedBuffer() {
      return unpinned_list.poll();
   }
}