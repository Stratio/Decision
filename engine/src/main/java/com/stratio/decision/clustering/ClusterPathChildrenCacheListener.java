/**
 * Copyright (C) 2014 Stratio (http://stratio.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
