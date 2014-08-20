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
package com.stratio.streaming.shell.commands;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.springframework.context.ApplicationContext;
import org.springframework.shell.Bootstrap;
import org.springframework.shell.core.JLineShellComponent;
import org.springframework.util.FileCopyUtils;

import com.stratio.streaming.api.IStratioStreamingAPI;

public class BaseShellTest {

    public static JLineShellComponent shell;

    public static IStratioStreamingAPI stratioStreamingApi;

    protected static ApplicationContext applicationContext;

    public void init() {
        Bootstrap bootstrap = new Bootstrap(null,
                new String[] { "classpath*:/META-INF/spring/spring-shell-plugin-test.xml" });
        shell = bootstrap.getJLineShellComponent();
        applicationContext = bootstrap.getApplicationContext();
        stratioStreamingApi = applicationContext.getBean(IStratioStreamingAPI.class);
    }

    public String getListResultFromName(String filename) throws IOException {
        String content = FileCopyUtils.copyToString(new BufferedReader(new InputStreamReader(this.getClass()
                .getResourceAsStream("/".concat(filename).concat(".txt")))));
        return content;
    }
}
