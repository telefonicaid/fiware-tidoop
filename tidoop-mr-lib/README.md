#<a name="top"></a>Tidoop - MapReduce job library

* [What is tidoop-mr-lib](#whatis)
* [Building](#mainbuilding)
    * [Prerequisites](#prerequisites)
    * [Building tidoop-mr-lib and its dependencies](#building)
    * [Unit tests](#unittests)
* [Installation](#install)
* [Usage](#usage)
    * [Running in the command line](#commandline)
    * [Oozie-based running](#oozie)
    * [Using tidoop-mr-lib-api](#mrlibapi)
* [Library details](#details)
    * [About input and output](#inputoutput)
    * [Job categories](#categories)
        * [Transformation and computation jobs](#transformationcomputation)
        * [Generic and NGSI specific jobs](#genericngsi)
        * [Flat and key-value jobs](#flatkeyvalue)
    * [Chaining of jobs](#chaining)
* [Jobs reference (in alphabetical order)](#jobs)
    * [Filter](#filter)
    * [MapOnly](#maponly)
* [Contact](#contact)

##<a name="whatis"></a>What is tidoop-mr-lib

tidoop-mr-lib is a library of already developed-ready to use [MapReduce](http://research.google.com/archive/mapreduce.html) jobs for [Hadoop](https://hadoop.apache.org), allowing data scientists and engineers in general for an easy and quick approach to Big Data analysis.

Available jobs can be used either to transform an input dataset, either to compute a value from it. Usually, these transformations or computations are suited for any general dataset, nevertheless there are NGSI specific MapReduce jobs simplifying a lot the process of analyzing NGSI-like context data. Have a look on this and many other ways of classifying the jobs in the [library details](#details) section.

Finally, it is worth to mention MapReduce jobs available within this library can be combined in a chain of executions, linking the outout of a job to another one's input. Job chaining can be done from the command line, manually running jobs sequentially, using [tidoop-mr-lib-api](../tidoop-mr-lib-api) or using [Apache Oozie](http://oozie.apache.org/). See the [chaining of jobs](#chaining) section for further details.

[Top](#top)

##<a name="mainbuilding"></a>Building
###<a name="prerequisites"></a>Prerequisites

Maven (and thus Java SDK, since Maven is a Java tool) is needed in order to build tidoop-hadoop-ext.

In order to install Java SDK (not JRE) version 1.6.0 or higher, just type one of the following options (as a sudoer):

    $ sudo yum install java-1.6.0-openjdk-devel   (JDK 6 for Fedora, RHEL, CentOS machines)
    $ sudo yum install java-1.7.0-openjdk-devel   (JDK 7 for Fedora, RHEL, CentOS machines)
    $ sudo apt-get install openjdk-6-jdk          (JDK 6 for Debian, Ubuntu machines)
    $ sudo apt-get install openjdk-7-jdk          (JDK 7 for Debian, Ubuntu machines)

Please observe [Open JDK](http://openjdk.java.net/install/) is installed. Remember to export the JAVA_HOME environment variable, just type one of the following options:

    $ export JAVA_HOME=/usr/lib/jvm/java-1.6.0-openjdk.x86_64   (JDK 6 for Fedora, RHEL, CentOS machines)
    $ export JAVA_HOME=/usr/lib/jvm/java-1.7.0-openjdk.x86_64   (JDK 7 for Fedora, RHEL, CentOS machines)
    $ export JAVA_HOME=/usr/lib/jvm/java-6-openjdk              (JDK 6 for Debian, Ubuntu machines)
    $ export JAVA_HOME=/usr/lib/jvm/java-7-openjdk              (JDK 7 for Debian, Ubuntu machines)

In order to do it permanently, edit `/root/.bash_profile` (`root` user) or `/etc/profile` (other users).

Maven (3.2.5 or higher) is installed by downloading it from [maven.apache.org](http://maven.apache.org/download.cgi). Install it in a folder of your choice (represented by `APACHE_MAVEN_HOME`):

    $ wget http://www.eu.apache.org/dist/maven/maven-3/3.2.5/binaries/apache-maven-3.2.5-bin.tar.gz
    $ tar xzvf apache-maven-3.2.5-bin.tar.gz
    $ mv apache-maven-3.2.5 APACHE_MAVEN_HOME

[Top](#top)

###<a name="building"></a>Building tidoop-mr-lib and its dependencies

Building the project can be done by including the dependencies in the package (**recommended**):

    $ git clone https://github.com/telefonicaid/fiware-tidoop.git
    $ cd fiware-tidoop/tidoop-mr-lib
    $ git checkout <branch>
    $ APACHE_MAVEN_HOME/bin/mvn clean compile assembly:single

or not:

    $ git clone https://github.com/telefonicaid/fiware-tidoop.git
    $ cd fiware-tidoop/tidoop-mr-lib
    $ git checkout <branch>
    $ APACHE_MAVEN_HOME/bin/mvn package

where `<branch>` is `develop` if you are installing the latest features or `release/x.y.z` if you are installing a stable release.

If the dependencies are included in the built tidoop-mr-lib package, then nothing has to be done. If not, and depending on the tidoop-mr-lib components you are going to use, you may need to install additional .jar files somewhere in the classpath. Typically, you can get the .jar file from your Maven repository (under `/home/<your_user>/.m2` folder in your user home directory) and use the `cp` command.

Once you have built tidoop-mr-lib a `tidoop-mr-lib-x.y.z-jar-with-dependencies.jar` / `tidoop-mr-lib-x.y.z.jar` (depending on wheter the dependencies are packaged or not, respectively) file will appear under `target/` folder. This jar is the file you will need to add to your Hadoop classpath in order to use the tidoop-mr-lib. 

[Top](#top)

###<a name="unittests"></a>Unit tests

Run them by invoking Maven:

    $ cd fiware-tidoop/tidoop-mr-lib
    $ /usr/local/apache-maven-3.2.5/bin/mvn test
   
Example:
 
```
$ APACHE_MAVEN_HOME/bin/mvn test
[INFO] Scanning for projects...
[INFO]                                                                         
[INFO] ------------------------------------------------------------------------
[INFO] Building tidoop-mr-lib 0.0.0-SNAPSHOT
[INFO] ------------------------------------------------------------------------
[INFO] 
[INFO] --- maven-resources-plugin:2.5:resources (default-resources) @ tidoop-mr-lib ---
[debug] execute contextualize
[INFO] Using 'UTF-8' encoding to copy filtered resources.
[INFO] Copying 0 resource
[INFO] 
[INFO] --- maven-compiler-plugin:2.3.2:compile (default-compile) @ tidoop-mr-lib ---
[INFO] Nothing to compile - all classes are up to date
[INFO] 
[INFO] --- maven-resources-plugin:2.5:testResources (default-testResources) @ tidoop-mr-lib ---
[debug] execute contextualize
[INFO] Using 'UTF-8' encoding to copy filtered resources.
[INFO] skip non existing resourceDirectory /Users/frb/devel/fiware/fiware-tidoop/tidoop-mr-lib/src/test/resources
[INFO] 
[INFO] --- maven-compiler-plugin:2.3.2:testCompile (default-testCompile) @ tidoop-mr-lib ---
[INFO] Nothing to compile - all classes are up to date
[INFO] 
[INFO] --- maven-surefire-plugin:2.10:test (default-test) @ tidoop-mr-lib ---
[INFO] Surefire report directory: /Users/frb/devel/fiware/fiware-tidoop/tidoop-mr-lib/target/surefire-reports

-------------------------------------------------------
 T E S T S
-------------------------------------------------------
Running com.telefonica.iot.tidoop.mrlib.FilterTest
Tests run: 5, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 0.439 sec

Results :

Tests run: 5, Failures: 0, Errors: 0, Skipped: 0

[INFO] ------------------------------------------------------------------------
[INFO] BUILD SUCCESS
[INFO] ------------------------------------------------------------------------
[INFO] Total time: 2.711s
[INFO] Finished at: Wed Jun 03 13:34:21 CEST 2015
[INFO] Final Memory: 6M/81M
[INFO] ------------------------------------------------------------------------
```

[Top](#top)

##<a name="install"></a>Installation

You can directly use the built `tidoop-mr-lib-x.y.z-jar-with-dependencies.jar` / `tidoop-mr-lib-x.y.z.jar` file (depending on wether you pack the dependencies or not, respectively) andunder the `target` directory, or you can copy the jar file somewhere in the Java classpath.

[Top](#top)

##<a name="usage"></a>Usage
###<a name="commandline"></a>Running in the command line
As usual in Hadoop's MapReduce, any job from tidoop-mr-lib can be run from the command line. Dependind on the chosen job, the arguments may vary, but it always will have at least an input folder and and an output folder.

    $ hadoop jar tidoop-mr-lib-x.y.z-jar-with-dependencies.jar <job_name> -libjars <list_of_dependency_jars> <argument0> <argument1> ...

[Top](#top)

###<a name="oozie"></a>Oozie-based running
To be done

[Top](#top)

###<a name="mrlibapi"></a>Using tidoop-mr-lib-api
There exists a parallel project to tidoop-mr-lib, named tidoop-mr-lib-api, in charge of exposing a REST-based method for running each MapReduce job within this library.

Please refer to the [usage](../tidoop-mr-lib-api/README.md#usage) section in the README of tidoop-mr-lib-api at Github.

[Top](#top)

##<a name="details"></a>Library details
###<a name="inputoutput"></a>About input and output

Hadoop works at the level of directory. This means any MapReduce job (but not only MapReduce, Hive behaves the same way) must specify as input and output a HDFS folder, avoiding to provide specific files.

From the input point of view, an input directory may contain several files. Thus a requirement for tidoop-mr-lib (and Hadoop in general) is all the files contain data about the same source, with common format, common purpose, as if all the data within them was a single data file. By accomplishing this, then an input folder can be seen as an input dataset, independently of the number of files it comprises.

From the output point of view, something similar occurs. The output of a MapReduce job comprises several files, one per each running reducer. In addition, some metadata is added to the folder, such as logs and an epty file whose name describes the returning status (`_SUCCESS` or not). It does not matter: the entire folder can be seen as an output dataset (an input dataset for another job) since all the files generated by the reducers will share the same format and purpose.

[Top](#top)

###<a name="categories"></a>Job categories
####<a name="transformationcomputation"></a>Transformation and computation jobs

On the one hand there are <i><b>transformation</b></i> jobs, i.e. MapReduce jobs that apply on most of the data that can be managed in a data science project. For instance, there is a transformation job aboult filtering lines in the input data based on a regular expression; another example could be the job allowing for joining two datasets into a single one.

On the other hand there are <i><b>computation</b></i> jobs in charge of returning a value which is function of the input data. Examples of this kind of operation could be retrieving the number of data lines, or performing some aggregation operation on the dataset, such as adding it (obviously, if the dataset contains numerical information). Output value can be appended to an output dataset, either as it is returned by the computation job, either by prepending a key value.

[Top](#top)

####<a name="genericngsi"></a>Generic and NGSI specific jobs

Of special interest are <i><b>NGSI specific</b></i> jobs that are only valid for NGSI-like datasets, such as those created by [Cygnus](https://github.com/telefonicaid/fiware-cygnus). Usually, these jobs have their <i><b>generic</b></i> counterpart, but the specific format of NGSI-like data (a JSON document with several custom fields) makes very hard to obtain the desired transformation or computation. For instance, computing the average of a certain entity's attribute will require at least a map job on all the NGSI data lines in order to obtain the desired value, and then the average job itself; nevertheless, there exists a single NGSI job dealing with such a computation. Another example could be filtering by entity type, which is a transformation: by using the generic version, a complex regular expresion must be used not only covering the entity type value itself but also covering the search for the field name containing the entity type; the NGSI version only requires a regular expresion for the entity type.

[Top](#top)

####<a name="flatkeyvalue"></a>Flat and key-value jobs

Finally, there are (transformation, NGSI or not) jobs that add keys as part of their functionallity, while there are other (transformation, NGSI or not) jobs not adding keys at all. There are also other (computation, NGSI or not) jobs allowing to add a custom key to the output they generate.

[Top](#top)

###<a name="chaining"></a>Chaining of jobs

To be done

[Top](#top)

##<a name="jobs"></a>Jobs reference (in alphabetical order)
###<a name="filter"></a>Filter
Use this job in order to filter lines within input data based on a regular expression. If the regular expression matches, then the line is maintained in the output data. For instance, assuming a file within the input folder containing this data:

    these are some lines of data
    this is not real data
    but it is useful for demostration purposes
    
If a regular expression such as `^.*\bdata\b.*$` was used, then the output file would be:

    these are some lines of data
    this is not real data

Arguments:

* **Input folder**: a HDFS folder containing the input data in the form of one or more files.
* **Output folder**: a HDFS folder containing the output data in the form of one or more files.
* **Regular expression**: a regular expression written in [Java format](http://docs.oracle.com/javase/7/docs/api/java/util/regex/Pattern.html).

Usage:

    $ hadoop jar \
         target/tidoop-mr-lib-x.y.z-jar-with-dependencies.jar \
         com.telefonica.iot.tidoop.mrlib.Filter \
         -libjars target/tidoop-mr-lib-x.y.z-jar-with-dependencies.jar \
         <input folder> \
         <output folder> \
         <regex>
         
Categories: **transformation**, **generic**, **flat**.

[Top](#top)

###<a name="maponly"></a>MapOnly
Use this job in order to apply a user defined function over all the input data (this is called a <i>map</i>). For instance, assuming a file within the input folder containing this data:

    3
    12
    5
    1.2
    410

If a mapping function such as `double y = x * x` was defined (`x` gets the value within each line), then the output file would be:

    9.0
    144.0
    25.0
    1.44
    168100.0
    
Nevertheless, if a mapping function such as `int y = x * x` was defined, then the output would change to:

    9
    144
    25
    168100
    
This is because `int y = 1.44 * 1.44` is not a valid statement in Java, due to types incompatibility (possible lossy conversion from `double` to `int`). As can be seen, the whole map is not stopped when a single evaluation cannot be done.

Please observe a string function can also be defined (when using string functions no types incompatibility arise since everything is text in HDFS), e.g. `String y = x + "_sufix"`:

    3_sufix
    12_sufix
    5_sufix
    1.2_sufix
    410_sufix

Even, a sequence of functions can be evaluated. In this case, the first function sets the type of the input data, and its result is stored in a temporal variable; the last function sets the type of the output data and its results is stored in the usual `y` variable. For instance, `double w = x * x; double z = w + 1; String y = z + "_sufix"` will produce the following output data:

    10_sufix
    145_sufix
    26_sufix
    2.44_sufix
    168101_sufix

Arguments:

* **Input folder**: a HDFS folder containing the input data in the form of one or more files.
* **Output folder**: a HDFS folder containing the output data in the form of one or more files.
* **Map function**: a function (or sequence of functions) written in Java language. Its format **must** be `'<type> y = <function of x>;'` (use `'` to enclose the function). The type is needed for checking types compatibility. Supported types from Java are `int`, `long`, `float`, `double`, `boolean` and `String`. If no type is given, or the type is unknown then the identity function (`String y = x`) is used instead. If no function is given or it is not a function of `x`, then the identity function (`String y = x`) is used instead.

Usage:

    $ hadoop jar \
         target/tidoop-mr-lib-x.y.z-jar-with-dependencies.jar \
         com.telefonica.iot.tidoop.mrlib.MapOnly \
         -libjars target/tidoop-mr-lib-x.y.z-jar-with-dependencies.jar \
         <input folder> \
         <output folder> \
         <map function>
         
Categories: **transformation**, **generic**, **flat**.

[Top](#top)

##<a name="contact"></a>Reporting issues and contact information
There are several channels suited for reporting issues and asking for doubts in general. Each one depends on the nature of the question:

* Use [stackoverflow.com](http://stackoverflow.com) for specific questions about the software. Typically, these will be related to installation problems, errors and bugs. Development questions when forking the code are welcome as well. Use the `fiware-tidoop` tag.
* Use [fiware-tech-help@lists.fi-ware.org](mailto:fiware-tech-help@lists.fi-ware.org) for general questions about the software. Typically, these will be related to the conceptual usage of the component, e.g. wether it suites for your project or not. It is worth to mention the issues reported to [fiware-tech-help@lists.fi-ware.org](mailto:fiware-tech-help@lists.fi-ware.org) are tracked under [http://jira.fiware.org](http://jira.fiware.org); use this Jira to see the status of the issue, who has been assigneed to, the exchanged emails, etc, nevertheless the answers will be sent to you via email too.
* Personal email:
    * [francisco.romerobueno@telefonica.com](mailto:francisco.romerobueno@telefonica.com) **[Main contributor]**
    * [fermin.galanmarquez@telefonica.com](fermin.galanmarquez@telefonica.com) **[Contributor]**
    * [german.torodelvalle@telefonica.com](german.torodelvalle@telefonica.com) **[Contributor]**

**NOTE**: Please try to avoid personaly emailing the contributors unless they ask for it. In fact, if you send a private email you will probably receive an automatic response enforcing you to use [stackoverflow.com](stackoverflow.com) or [fiware-tech-help@lists.fi-ware.org](mailto:fiware-tech-help@lists.fi-ware.org). This is because using the mentioned methods will create a public database of knowledge that can be useful for future users; private email is just private and cannot be shared.

[Top](#top)
