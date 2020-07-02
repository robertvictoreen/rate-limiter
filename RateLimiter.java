import java.util.*;
import java.util.concurrent.*;
class RateLimiter {
    public static void main(String args[] ) throws Exception {
        // Rate limit 5 messages in a 5ms window
        RateLimiter rl = new RateLimiter(5, 5);

        // Messages will be sent
        rl.sendMessage(1000);
        rl.sendMessage(1001);
        rl.sendMessage(1002);
        rl.sendMessage(1003);
        rl.sendMessage(1004);

        // Messages be rate limited
        rl.sendMessage(1005);
        rl.sendMessage(1006);
        rl.sendMessage(1007);
        rl.sendMessage(1008);
        rl.sendMessage(1009);

        // Messages will be sent
        rl.sendMessage(2000);
        rl.sendMessage(2001);
        rl.sendMessage(2002);
        rl.sendMessage(2003);
        rl.sendMessage(2004);

        // Messages be rate limited
        rl.sendMessage(2005);
        rl.sendMessage(2006);
        rl.sendMessage(2007);
        rl.sendMessage(2008);
        rl.sendMessage(2009);

        // Wait for responses
        Thread.sleep(10000);

        // Print 10 messages sent
        rl.printNumberOfMessagesSent();
    }
    
    int limitNumberOfMessages;
    int timeWindowMs;
    int insertionPoint;
    int messagesSent;
    int size;
    int[] history;
    
    RateLimiter(int limitNumberOfMessages, int timeWindowMs) {
        history = new int[limitNumberOfMessages];
        insertionPoint = 0;
        messagesSent = 0;
        size = 0;
        this.limitNumberOfMessages = limitNumberOfMessages;
        this.timeWindowMs = timeWindowMs;
    }

    public void printNumberOfMessagesSent() {
        System.out.println("Messages Sent: " + messagesSent);
    }

    private synchronized void updateNumberOfMessagesSent() {
        messagesSent++;
    }

    private synchronized int aquireSlot(int curTimeMs) {
        int slot = insertionPoint;
        System.out.println("Aquired Slot for 500ms: "+slot);
        recordSlot(curTimeMs + 500, slot);
        insertionPoint++;
        if (insertionPoint == history.length) {
            insertionPoint = 0;
        }
        if (size < limitNumberOfMessages) {
            size++;
        }
        return slot;
    }

    private synchronized void recordSlot(int responseTimeMs, int slot) {
        history[slot] = responseTimeMs;
    }

    public synchronized boolean isAllowed(int curTimeMs) {
        if (size < limitNumberOfMessages) {
            return true;
        }

        int firstWindowSlot = insertionPoint - limitNumberOfMessages;
        if (firstWindowSlot < 0) {
            firstWindowSlot = limitNumberOfMessages + firstWindowSlot;
        }
        
        int prevTimeMs = history[firstWindowSlot];
        
        if (curTimeMs - prevTimeMs <= timeWindowMs) {
            return false;
        }
        return true;
        
    }

    private void sendMessageToServer(int curTimeMs, int slot) {
        try {
            System.out.println("Sent message at: " + curTimeMs);
            Thread.sleep(100);
            int responseTimeMs = curTimeMs + 1000;
            updateNumberOfMessagesSent();
            System.out.println("Recieved response at: " + responseTimeMs);
            recordSlot(responseTimeMs, slot);
        } catch (InterruptedException e) {
            System.out.println(e);
        }
    }

    public synchronized boolean sendMessage(final int curTimeMs) {
        if (!isAllowed(curTimeMs)) {
            System.out.println("Message rate limited at: "+ curTimeMs);
            return false;
        }
        final int slot = aquireSlot(curTimeMs);

        CompletableFuture.runAsync(() -> sendMessageToServer(curTimeMs, slot));
        
        return true;
    }
}
