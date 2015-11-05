package com.stratio.decision.testsAT.automated;

import org.testng.annotations.Test;

import com.stratio.cucumber.testng.CucumberRunner;
import com.stratio.decision.testsAT.utils.BaseTest;

import cucumber.api.CucumberOptions;

@CucumberOptions(features = { "src/test/resources/features/automated/ShellHelp.feature" })
public class ATShHelp extends BaseTest {

    public ATShHelp() {
    }

    @Test(enabled = true, priority = 1)
    public void shellHelpTest() throws Exception {
        new CucumberRunner(this.getClass()).runCukes();
    }
}
