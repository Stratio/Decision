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
package com.stratio.decision.api.partitioner;

/**
 * Created by josepablofernandez on 7/03/16.
 */
public class PartitionerStrategyFactory {

    public enum Strategy {
        HASH;
    }

    private IPartitionerStrategy hashPartitionerStrategy = null;

    public IPartitionerStrategy getPartitionerInstance(Strategy strategy) {

        IPartitionerStrategy partitioner = null;

        switch (strategy) {

            case HASH:  if (hashPartitionerStrategy == null) {
                            hashPartitionerStrategy = new HashPartitionerStrategy();
                        }
                        partitioner = hashPartitionerStrategy;
                        break;
            default: partitioner = hashPartitionerStrategy;
        }

        return partitioner;

    }

}
