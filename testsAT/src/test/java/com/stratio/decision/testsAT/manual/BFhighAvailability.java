package com.stratio.decision.testsAT.manual;

import com.stratio.cucumber.testng.CucumberRunner;
import com.stratio.decision.testsAT.utils.BaseTest;
import cucumber.api.CucumberOptions;
import org.testng.annotations.Test;

@CucumberOptions(features = { "src/test/resources/features/BFstreamSaveToMongo" })
public class BFhighAvailability extends BaseTest {

    public BFhighAvailability() {
    }

    @Test(enabled = true, groups = {"manual"})
    public void manualTest() throws Exception {
        new CucumberRunner(this.getClass()).runCukes();
    }
}
