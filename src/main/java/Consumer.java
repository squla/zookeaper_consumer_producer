import org.apache.log4j.Logger;
import org.apache.zookeeper.*;

import java.util.List;

/**
 * Created by Michal on 2016-03-10.
 */
public class Consumer implements Watcher, Runnable {
    private Logger _log = Logger.getLogger(Consumer.class);
    private ZooKeeper _zookeeperClient;
    private Boolean remove;

    public Consumer() throws Exception {
        _zookeeperClient = new ZooKeeper("localhost:2181", 3000, this);
        remove = false;
    }

    public Consumer(Boolean remove) throws Exception {
        this();
        this.remove = remove;
    }


    private List<String> getChildren() throws Exception {
        return _zookeeperClient.getChildren("/tasks", this);
    }


    public void process(WatchedEvent watchedEvent) {
        if (watchedEvent.getType() == Event.EventType.NodeChildrenChanged) {
            try {
                _log.info("List Tasks: " + this.getChildren().toString());
            } catch (Exception e) {
                _log.error("Consumer process throw Exception: " + e.getMessage());
            }
        }
//        else if(watchedEvent.getType() == Event.EventType.None && watchedEvent.getState() == Event.KeeperState.Expired){
//            return;
//        }
    }


    public void run() {
        try {
            while (true) {

                    List<String> children = getChildren();
                   if (remove && children.size() > 0){
                        for (String s : children) {
                            _log.info("Consume task: " + s + " data: " + new String(_zookeeperClient.getData("/tasks/" + s, false, null)));
                            _zookeeperClient.delete("/tasks/" + s, 0);
                            Thread.sleep(1000);
                        }
                    }
            }
        } catch (Exception e) {
            _log.error("Consumer run() throw Exception: " + e.getMessage());
        }
    }

//    public void run(){
//        while (true){
//            try {
//                getChildren();
//                Thread.sleep(1000);
//            }catch (Exception e){
//
//            }
//        }
//    }


    public static void main(String args[]) throws Exception {
        Consumer consumer = new Consumer();
        Thread thread = new Thread(consumer);
        thread.start();

    }

}

