import org.apache.log4j.Logger;
import org.apache.zookeeper.*;

import java.io.IOException;

/**
 * Created by Michal on 2016-03-10.
 */
public class Producer implements Watcher {

    Logger _log = Logger.getLogger(Producer.class);
    private ZooKeeper _zookeeperClient;

    public Producer() throws IOException {
        _zookeeperClient = new ZooKeeper("localhost:2181", 3000, this);
    }

    private void createNodeASyncCall(final String jobNumber) {
        AsyncCallback.StringCallback createResponseHandler = new AsyncCallback.StringCallback() {
            public void processResult(int rc, String path, Object ctx, String name) {
                switch (KeeperException.Code.get(rc)) {
                    case OK:
                        _log.info("TASK CREATED - " + name);
                        break;
                    case CONNECTIONLOSS:
                    case OPERATIONTIMEOUT:
                    case NONODE:
                        _log.warn("Could not create task : will retry - " + KeeperException.Code.get(rc));
                        createNodeASyncCall(jobNumber);
                    default:
                        _log.error("ERROR CREATING TASK - " + KeeperException.Code.get(rc));
                        break;
                }
            }
        };
        _zookeeperClient.create("/tasks/task-", jobNumber.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT_SEQUENTIAL, createResponseHandler, "CreateTask");
    }

    private boolean isEgzists() throws Exception {
        return _zookeeperClient.exists("/goAhead", true) != null;
    }

    public void run() throws Exception {
        int counter = 0;
        while (true) {
            if(isEgzists()) {
                this.createNodeASyncCall("job_" + counter++);
            }else{
                _log.info("/goAhead node no exists");
            }
            Thread.sleep(10000);
        }
    }

    public void process(WatchedEvent watchedEvent) {

    }

    public static void main(String args[]) throws Exception {
        Producer producer = new Producer();
        producer.run();
    }
}
