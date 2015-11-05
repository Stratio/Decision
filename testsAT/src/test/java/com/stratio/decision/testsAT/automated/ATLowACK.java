package com.stratio.decision.testsAT.automated;

import org.testng.annotations.Test;

import com.stratio.cucumber.testng.CucumberRunner;
import com.stratio.decision.testsAT.utils.BaseTest;

import cucumber.api.CucumberOptions;

@CucumberOptions(features = { "src/test/resources/features/automated/StreamLowACK.feature" })
public class ATLowACK extends BaseTest {

    public ATLowACK() {
    }

    @Test(enabled = true, priority = 999)
    public void lowACKTest() throws Exception {
        new CucumberRunner(this.getClass()).runCukes();
    }
}
