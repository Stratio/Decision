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
package com.stratio.decision.shell.commands;

import com.stratio.decision.shell.provider.DefaultFileNameProvider;
import com.stratio.decision.shell.provider.StratioStreamingBannerProvider;
import com.stratio.decision.shell.provider.StratioStreamingPromptProvider;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Created by eruiz on 29/09/15.
 */
public class ProviderTest extends BaseShellTest{



    @Before
    public void setUp() {

        init();
    }
    StratioStreamingBannerProvider bannerProvider = new StratioStreamingBannerProvider();
    StratioStreamingPromptProvider promptProvider = new StratioStreamingPromptProvider();
    DefaultFileNameProvider nameProvider=new DefaultFileNameProvider();

    @Test
    public void testGetProviderName() throws Exception {
        String name= bannerProvider.getProviderName();
        assertEquals("Stratio Decision",name);
    }

    @Test
    public void testGetWelcomeMessage() throws Exception {
        String wm= bannerProvider.getWelcomeMessage();
        assertEquals("Type \"help\" to see all available commands.",wm);
    }
    @Test
    public void testGetVersion() throws Exception {
        String v= bannerProvider.getVersion();
        assertEquals("1",v);
    }
    @Ignore
    @Test
    public void testGetBanner() throws Exception {
        String banner= bannerProvider.getBanner();
        assertEquals(getListResultFromName("bannerResult"),banner);
    }
    @Test
    public void testGetPrompt() throws Exception {
        String prompt= promptProvider.getPrompt();
        assertEquals("stratio-decision> ",prompt);
    }
    @Test
    public void testGetProviderName2() throws Exception {
        String name= promptProvider.getProviderName();
        assertEquals("Stratio Decision",name);
    }
    @Test
    public void testGetProviderName3() throws Exception {
        String name= nameProvider.getProviderName();
        assertEquals("History filename provider",name);
    }

}
