/*
 * Copyright 2014 Stratio.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import sbt._
import Keys._

object MainBuild extends Build {
  lazy val root =
    Project("root", file("."))
      .configs( IntegrationTest )
      .settings( inConfig(IntegrationTest)(Defaults.testTasks) : _*)
      .settings(
      testOptions in Test := Seq(Tests.Filter(unitFilter)),
      testOptions in IntegrationTest := Seq(Tests.Filter(itFilter))
    )

  def itFilter(name: String): Boolean = name endsWith "IntegrationTests"
  def unitFilter(name: String): Boolean = (name endsWith "UnitTests") && !itFilter(name)

  lazy val IntegrationTest = config("integration") extend(Test)
}