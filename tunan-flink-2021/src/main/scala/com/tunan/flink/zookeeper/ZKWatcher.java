package com.tunan.flink.zookeeper;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.*;
import org.apache.curator.retry.ExponentialBackoffRetry;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @Auther: 李沅芮
 * @Date: 2021/12/23 09:19
 * @Description:
 */
public class ZKWatcher {

    //会话超时时间
    private static final int SESSION_TIMEOUT = 10 * 1000;

    //连接超时时间
    private static final int CONNECTION_TIMEOUT = 3 * 1000;

    //ZooKeeper服务地址
    private static final String CONNECT_ADDR = "hadoop1:2181";

    //创建连接实例
    private static CuratorFramework client ;

    static {
        //1 重试策略：重试时间为1s 重试10次
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 5);
        //2 通过工厂创建连接
        client = CuratorFrameworkFactory.builder()
                .connectString(CONNECT_ADDR).connectionTimeoutMs(CONNECTION_TIMEOUT)
                .sessionTimeoutMs(SESSION_TIMEOUT)
                .retryPolicy(retryPolicy)
                .build();
        //3 开启连接
        client.start();
    }

    public static void main(String[] args) throws Exception {

        /**
         * 在注册监听器的时候，如果传入此参数，当事件触发时，逻辑由线程池处理
         */
        ExecutorService pool = Executors.newFixedThreadPool(2);

        /**
         * 监听数据节点的变化情况
         */
//        final NodeCache nodeCache = new NodeCache(client, "/curator/table", false);
//        nodeCache.start(true);
//        nodeCache.getListenable().addListener(
//                new NodeCacheListener() {
//                    @Override
//                    public void nodeChanged() throws Exception {
//                        System.out.println("Node data is changed, new data: " +
//                                new String(nodeCache.getCurrentData().getData()));
//                    }
//                },
//                pool
//        );

        /**
         * 监听子节点的变化情况
         */
        final PathChildrenCache childrenCache = new PathChildrenCache(client, "/curator", true);
        childrenCache.start(PathChildrenCache.StartMode.POST_INITIALIZED_EVENT);
        childrenCache.getListenable().addListener(
                new PathChildrenCacheListener() {
                    @Override
                    public void childEvent(CuratorFramework client, PathChildrenCacheEvent event)
                            throws Exception {
                        switch (event.getType()) {
                            case CHILD_ADDED:
                                System.out.println("添加子节点: " + event.getData().getPath() +" "+ new String(event.getData().getData()));
                                break;
                            case CHILD_REMOVED:
                                System.out.println("删除子节点: " + event.getData().getPath() +" "+ new String(event.getData().getData()));
                                break;
                            case CHILD_UPDATED:
                                System.out.println("修改子节点: " + event.getData().getPath() +" "+ new String(event.getData().getData()));
                                break;
                            default:
                                break;
                        }
                    }
                },
                pool
        );

        Thread.sleep(Integer.MAX_VALUE);
        pool.shutdown();
        client.close();

    }
}
