---
title: Writing and Running a Basic Application for Stratio Streaming
---

In this tutorial you will learn how to write a java or scala project for building Stratio Streaming applications 
and how to run it on a local instance or a standalone cluster. Instructions are based on the Eclipse environment 
but any equivalent can be used.

Table of Contents
=================

-   [Before you start](#before-you-start)
    -   [Prerequisites](#prerequisites)
    -   [Resources](#resources)
-   [Creating the project](#creating-the-project)
    -   [Step 1: Create an empty project](#step-1-create-an-empty-project)
    -   [Step 2: Import the project skeleton](#step-2-import-the-project-skeleton)
-   [Running the application](#running-the-application)
-   [Where to go from here](#where-to-go-from-here)

Before you start
================

Prerequisites
-------------

-   A [Stratio installation](/getting-started.html "Getting Started").
-   [Eclipse](https://www.eclipse.org/ "Eclipse website") or an equivalent IDE.
-   [Oracle JDK 7](http://www.oracle.com/technetwork/java/javase/downloads/index.html "Oracle Java7 download page").
-   [Apache Maven](http://maven.apache.org/ "The Maven project website"): Stratio Streaming API is available in a Maven repository that will be used in this tutorial.
-   [Scala](http://www.scala-lang.org/ "Scala website") >=2.10.3.
-   Scala-IDE: follow [instructions at Eclipse marketplace](http://marketplace.eclipse.org/marketplace-client-intro "Instructions to use Eclipse Marketplace") to install it from the marketplace (recommended over downloading the plugin from scala-ide.org).
-   m2eclipse-scala plugin: follow [instructions at scala-ide.org](http://scala-ide.org/docs/tutorials/m2eclipse/index.html "Tutorial for m2eclipse-scala plugin installation") for installation.

Resources
---------

Here is a list of the resources that will be used in this tutorial. You can download them now or as 
you go through the instructions. Links will be provided later as they will be needed.

-   [Java project example](http://docs.openstratio.org/resources/eclipse/streaming/StratioStreamingJavaProject.zip)
-   [Scala project example](http://docs.openstratio.org/resources/eclipse/streaming/StratioStreamingScalaProject.zip)

Creating the project
====================

Step 1: Create an empty project
-------------------------------

-   Launch Eclipse and in the menu choose File -> New -> Project
-   In the “New project” window select “Project” under “General” and click “Next”:

![Screenshot of the New Project window in Eclipse](http://www.openstratio.org/wp-content/uploads/2014/03/01-newProject.png)

-   In the next window, enter a name for your project and click “Finish”:

![Screenshot of the Project Name window in Eclipse](http://www.openstratio.org/wp-content/uploads/2014/03/02-projectName.png)

The newly created project now appears in the package explorer.

Step 2: Import the project skeleton
-----------------------------------

Download the project skeleton of your choice and save it in a convenient location:

-   [Java project](http://docs.openstratio.org/resources/eclipse/streaming/StratioStreamingJavaProject.zip)
-   [Scala project](http://docs.openstratio.org/resources/eclipse/streaming/StratioStreamingScalaProject.zip)

In the menu, choose File -> Import. In the “Import” window, select “Archive file” in the section “General”, and click “Next”:

![Screenshot of the Import window in Eclipse](http://www.openstratio.org/wp-content/uploads/2014/03/03-importWindow.png)

In the next screen:

-   Navigate to the zip file you just downloaded using the “Browse…” button.
-   Fill in “Into folder” with the name of the project (or use the “Browse…” button to select it from a list).
-   Check “Overwrite existing resources without warning”,
-   and click “Finish”

![Screenshot of the Importing from Archive file window in Eclipse](http://www.openstratio.org/wp-content/uploads/2014/03/04-importFromFile1.png)

The structure of the project will be displayed in the package explorer. Give Maven some time to check 
and download dependencies. The project should finally appear error-free.

The java project contains an example class (JavaExample.java) and an example test (TestJava.java), the 
scala one an example object (ScalaExample.app) and an example test (TestScala.scala), the mixed project 
contains all the formers.

Navigate through your project to get familiar with it. You can add your own code and optionally alter the 
stream, insert data, add queries and listeners.

Running the application
=======================

You can run the example provided in the project directly on your IDE. To do so, right click on the 
JavaExample.java file -> Run As -> Java Application.

For the Java or Scala project, the result should be similar to the following:

~~~~ {code}
Streams in the Stratio Streaming Engine: 3
-- Stream Name: stratio_stats_base
-- Stream Name: stratio_stats_global_by_operation
-- Stream Name: testStream
~~~~

Congratulations! You successfully completed this tutorial.

Where to go from here
=====================

If you are planning to write your own Stratio Streaming application, 
[these examples](using-streaming-api-examples.html "Using the Stratio Streaming API") may be useful. 
Those are snippets written in both Java and Scala.
