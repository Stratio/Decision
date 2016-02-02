package com.stratio.decision.clustering;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.utils.ZKPaths;

/**
 * Created by josepablofernandez on 2/02/16.
 */
public class ClusterPathChildrenCacheListener implements PathChildrenCacheListener {

    private ClusterSyncManager clusterSyncManagerInstance;

    public ClusterPathChildrenCacheListener(ClusterSyncManager clusterSyncManagerInstance){

        this.clusterSyncManagerInstance = clusterSyncManagerInstance;

    }

    @Override
    public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception
    {
        String node =  ZKPaths.getNodeFromPath(event.getData().getPath());
        String nodeId = node.substring(node.indexOf("_")+1);

        clusterSyncManagerInstance.updateNodeStatus(nodeId, event.getType());

    }
}
