package com.stratio.decision.clustering;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.utils.ZKPaths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by josepablofernandez on 2/02/16.
 */
public class ClusterPathChildrenCacheListener implements PathChildrenCacheListener {

    private static Logger logger = LoggerFactory.getLogger(ClusterPathChildrenCacheListener.class);

    private ClusterSyncManager clusterSyncManagerInstance;

    public ClusterPathChildrenCacheListener(ClusterSyncManager clusterSyncManagerInstance){

        this.clusterSyncManagerInstance = clusterSyncManagerInstance;

    }

    @Override
    public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception
    {
        String node;
        String nodeId;

        try {
            node = ZKPaths.getNodeFromPath(event.getData().getPath());
            nodeId = node.substring(node.indexOf("_") + 1);

            clusterSyncManagerInstance.updateNodeStatus(nodeId, event.getType());

        }catch (Exception e){
            logger.error("Exception receiving event {}: {}", event, e.getMessage());
        }

    }
}
