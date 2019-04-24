package com.google.zookeeper.client;

import org.I0Itec.zkclient.ZkClient;
import org.apache.zookeeper.CreateMode;

public class ZKClientDemo {

    public static void main(String[] args) throws Exception {
        ZkClient client = getClient();
        //listener(client);
        client.writeData("/zk-test-001/001","CountDownLatch");
        String a = client.create("/a", "a", CreateMode.PERSISTENT);
        String o = client.readData("/zk-test-001/001");
        System.out.println(o);
        Thread.sleep(100000);
    }

    private static void listener(ZkClient client) {
        client.subscribeChildChanges("/zk-test-001/001", (parentPath, currentChilds) -> {
            System.out.println(currentChilds);
        });
        client.createPersistent("/zk-test-001/001/3/1", true);//创建子节点,同时递归创建父节点
        client.createPersistent("/zk-test-001/001/4/1", true);//创建子节点,同时递归创建父节点
    }

    private static ZkClient getClient() {
        ZkClient zkClient = new ZkClient("192.168.88.128:2181",5000);
        System.out.println("client finished");
        return zkClient;
    }
}
