package com.stratio.decision.unit.engine.shell.commands;

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
        assertEquals("stratio-streaming> ",prompt);
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
