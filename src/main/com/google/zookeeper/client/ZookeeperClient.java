package com.google.zookeeper.client;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

public class ZookeeperClient implements Watcher {

    private static CountDownLatch countDownLatch = new CountDownLatch(1);
    private static CountDownLatch countDownLatch2 = new CountDownLatch(1);
    private static ZooKeeper zk = null;
    private static Stat stat = new Stat();

    public static void main(String[] args) throws Exception {
        getClient();
        System.out.println(exists("/zk-test-001/002"));

        Thread.sleep(100000L);
    }

    private static boolean exists(String path) {
        try {
            Stat stat = zk.exists(path, false);
            if (stat != null) {
                System.out.println("stat version:" + stat.getVersion());
            }
            return stat != null;
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }
        return false;
    }

    private static void updateData(String path, String data) {
        try {
            int version = stat.getVersion();
            System.out.println("version:" + version);
            stat = zk.setData(path, data.getBytes(), version);
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    private static byte[] getData() throws KeeperException, InterruptedException {
        byte[] data = zk.getData("/zk-test-001/001", true, stat);//获取数据同时注册watcher
        return data;
    }

    //获取子节点
    private static List<String> getChildren(ZooKeeper zooKeeper) {
        String parentPath = "/zk-test-001";
        try {
            List<String> children = zooKeeper.getChildren(parentPath, true, stat);
            return children;
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }
        return null;
    }

    private static String createPath2(ZooKeeper client) throws KeeperException, InterruptedException {
        Map<String, String> pathMap = new HashMap<>();
        client.create("/zk-test-001/001", "CountDownLatch".getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE//后续操作无需权限控制
                , CreateMode.PERSISTENT//临时节点
                , (rc, path, ctx, name) -> {
                    System.out.println("rc:" + rc + ",path:" + path + ",ctx:" + ctx + ",name:" + name);
                    pathMap.put("path", path);
                    countDownLatch2.countDown();
                }, "CountDownLatch2"
        );
        countDownLatch2.await();
        return pathMap.getOrDefault("path", null);
    }

    private static String createPath1(ZooKeeper client) throws KeeperException, InterruptedException {
        return client.create("/zk-test-001/002", "CountDownLatch".getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE//后续操作无需权限控制
                , CreateMode.EPHEMERAL//临时节点
        );
    }

    private static ZooKeeper getClient() throws IOException, InterruptedException {
        String connectString = "192.168.88.128:2181";
        ZooKeeper zooKeeper = new ZooKeeper(connectString, 100000, new ZookeeperClient());
        System.out.println("start state:" + zooKeeper.getState());
        countDownLatch.await();
        System.out.println("zookeeper session establish:" + zooKeeper.getState());
        zooKeeper.addAuthInfo("digest", "foo:true".getBytes());
        zk = zooKeeper;
        return zooKeeper;
    }

    //watcher事件处理
    public void process(WatchedEvent event) {
        System.out.println("watched event:" + event.getState());
        if (Event.KeeperState.SyncConnected == event.getState()) {
            if (Event.EventType.None == event.getType() && event.getPath() == null) {
                countDownLatch.countDown();
            } else if (event.getType() == Event.EventType.NodeChildrenChanged) {
                //子节点变更事件
                try {
                    System.out.println(zk.getChildren(event.getPath(), true));
                } catch (KeeperException | InterruptedException e) {
                    e.printStackTrace();
                }
            } else if (event.getType() == Event.EventType.NodeDataChanged) {
                //节点数据变更
                try {
                    System.out.println(event.getPath() + ",NodeDataChanged");
                    System.out.println(new String(zk.getData(event.getPath(), false, stat)) + ",version:" + stat.getVersion());
                } catch (KeeperException | InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
