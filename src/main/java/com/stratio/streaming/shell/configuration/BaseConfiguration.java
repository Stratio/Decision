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
package com.stratio.streaming.shell.configuration;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;

import com.stratio.streaming.api.IStratioStreamingAPI;
import com.stratio.streaming.commons.streams.StratioStream;
import com.stratio.streaming.shell.dao.CachedStreamsDAO;
import com.stratio.streaming.shell.dao.impl.CachedStreamsDAOImpl;
import com.stratio.streaming.shell.renderer.Renderer;
import com.stratio.streaming.shell.renderer.StreamListRenderer;

@Configuration
@Import({ StreamingApiConfiguration.class, CacheConfiguration.class })
public class BaseConfiguration {

    @Autowired
    private IStratioStreamingAPI stratioStreamingApi;

    @Bean
    public CachedStreamsDAO cachedStreamsDao() {
        return new CachedStreamsDAOImpl(stratioStreamingApi);
    }

    @Bean
    public Renderer<List<StratioStream>> stratioStreamRenderer() {
        return new StreamListRenderer();
    }

    @Bean
    public static PropertySourcesPlaceholderConfigurer propertySourcesPlaceholderConfigurer() {
        return new PropertySourcesPlaceholderConfigurer();
    }

}
