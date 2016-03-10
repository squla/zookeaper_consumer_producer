import org.apache.log4j.Logger;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.util.List;

/**
 * Created by Michal on 2016-03-10.
 */
public class Consumer implements Watcher, Runnable {
    private Logger _log = Logger.getLogger(Consumer.class);
    private ZooKeeper _zookeeperClient;

    public Consumer() throws Exception {
        _zookeeperClient = new ZooKeeper("localhost:2181", 3000, this);
    }


    private List<String> getChildren() throws Exception {
        return _zookeeperClient.getChildren("/tasks", this);
    }

    private Integer mutex = 1;

    public void process(WatchedEvent watchedEvent) {
        if (watchedEvent.getType() == Event.EventType.NodeChildrenChanged) {
            try {
                _log.info("List Tasks: " + this.getChildren().toString());
            } catch (Exception e) {
                _log.error("Consumer process throw Exception: " + e.getMessage());
            }
            synchronized (mutex) {
                mutex.notify();
            }
        }
    }


    public void run() {
        try {
            while (true) {
                synchronized (mutex) {
                    List<String> children = getChildren();
                    if (children.size() == 0) {
                        mutex.wait();
                    } else {
                        for (String s : children) {
                            _log.info("Consume task: " + s + " data: " + new String(_zookeeperClient.getData("/tasks/" + s, false, null)));
                            _zookeeperClient.delete("/tasks/" + s, 0);
                            Thread.sleep(100);
                        }
                    }
                    Thread.sleep(300);
                }
            }
        } catch (Exception e) {
            _log.error("Consumer run() throw Exception: " + e.getMessage());
        }
    }


    public static void main(String args[]) throws Exception {
        Consumer consumer = new Consumer();
        Thread thread = new Thread(consumer);
        thread.start();

    }

}
