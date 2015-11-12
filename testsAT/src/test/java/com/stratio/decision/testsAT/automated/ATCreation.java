package com.stratio.decision.testsAT.automated;

import org.testng.annotations.Test;

import com.stratio.cucumber.testng.CucumberRunner;
import com.stratio.decision.testsAT.utils.BaseTest;

import cucumber.api.CucumberOptions;

@CucumberOptions(features = { "src/test/resources/features/automated/StreamCreation.feature" })
public class ATCreation extends BaseTest {

    public ATCreation() {
    }

    @Test(enabled = true, priority = 1)
    public void createStreamTest() throws Exception {
        new CucumberRunner(this.getClass()).runCukes();
    }
}
