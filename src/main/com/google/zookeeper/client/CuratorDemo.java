package com.google.zookeeper.client;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.atomic.AtomicValue;
import org.apache.curator.framework.recipes.atomic.DistributedAtomicInteger;
import org.apache.curator.framework.recipes.barriers.DistributedDoubleBarrier;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.RetryNTimes;
import org.apache.zookeeper.CreateMode;

import java.util.Date;

public class CuratorDemo {

    private static String server = "192.168.88.128:2181";
    private static String fpath = "/node001";
    private static CuratorFramework client = CuratorFrameworkFactory.builder().
            connectString(server).
            sessionTimeoutMs(5000).
            retryPolicy((retryCount, elapsedTimeMs, sleeper) -> retryCount <= 3 && elapsedTimeMs <= 10000).
            //namespace(fpath).//根路径
                    build();

    public static void main(String[] args) throws Exception {
        client.start();//完成客户端(会话)创建
        listener();
    }


    private static void listener() throws Exception {
        String path = client.create().creatingParentContainersIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(fpath + "/001", "这谁顶得住啊".getBytes());
        System.out.println(path);
        NodeCache nodeCache = new NodeCache(client, path, false);
        //设置为true,nodecache第一次启动时就立刻从zk中读取数据,保存在cache中
        nodeCache.start(true);
        nodeCache.getListenable().addListener(() -> {
            System.out.println("node data updated.new data:" + new String(nodeCache.getCurrentData().getData()));
        });
        long mtime = nodeCache.getCurrentData().getStat().getMtime();
        System.out.println(new Date(mtime));
        System.out.println("start data:" + new String(nodeCache.getCurrentData().getData()));
        client.setData().forPath(fpath + "/001", "77777".getBytes());
        Thread.sleep(1000L);
        client.delete().deletingChildrenIfNeeded().forPath(fpath + "/001");
        Thread.sleep(2000L);
    }

    //master
    private static void master() throws Exception {
        String masterPath = "/curator_master_path";
        final String name = Thread.currentThread().getName();
        LeaderSelector selector = new LeaderSelector(client, masterPath, new LeaderSelectorListenerAdapter() {
            @Override
            public void takeLeadership(CuratorFramework client) throws Exception {
                System.out.println(name + "成为master");
                Thread.sleep(3000);
                System.out.println(name + "退出...");
            }
        });
        PathChildrenCache pathChildrenCache = new PathChildrenCache(client, masterPath, true);
        pathChildrenCache.start(PathChildrenCache.StartMode.POST_INITIALIZED_EVENT);
        pathChildrenCache.getListenable().addListener((client, event) -> {
            switch (event.getType()) {
                case CHILD_ADDED: {
                    System.out.println(name + "--->child add" + event.getData());
                }
                case CHILD_REMOVED: {
                    System.out.println(name + "--->child removed" + event.getData());
                }
                case CHILD_UPDATED: {
                    System.out.println(name + "child updated" + event.getData());
                }
            }
        });
        selector.autoRequeue();
        selector.start();
        Thread.sleep(5000000);
    }

    //分布式锁
    private static void lock() throws Exception {
        String lockPath = "/lockPath";

        InterProcessMutex lock = new InterProcessMutex(client, lockPath);
        final String name = Thread.currentThread().getName();
        lock.acquire();
        System.out.println(name + "acquire...");
        lock.release();
        System.out.println(name + "release...");
    }

    //计数器
    private static void count() throws Exception {
        String countPath = "/count_path";
        final String name = Thread.currentThread().getName();

        DistributedAtomicInteger atomicInteger = new DistributedAtomicInteger(client, countPath, new RetryNTimes(3, 1000));

        atomicInteger.initialize(10);
        AtomicValue<Integer> add = atomicInteger.add(10);
        System.out.println(name + add.succeeded());
        System.out.println(name + add.preValue());
        System.out.println(name + add.postValue());
    }

    //分布式barrier
    private static void barrier() throws Exception {
        String barrierPath = "/barrierPath";
        Thread.sleep(3000);
        for (int i = 0; i < 3; i++) {
            new Thread(() -> {
                try {
                    DistributedDoubleBarrier barrier = new DistributedDoubleBarrier(client, barrierPath, 3);
                    final String name = Thread.currentThread().getName();
                    barrier.enter();
                    System.out.println(name + "enter");
                    barrier.leave();
                    System.out.println(name + "leave");
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }).start();
        }

    }


}
