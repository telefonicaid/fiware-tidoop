#<a name="top"></a>Tidoop - MapReduce job library REST API

* [What is tidoop-mr-lib-api](#whatis)
* [Installation](#maininstall)
    * [Prerequisites](#prerequisites)
    * [API installation](#apiinstallation)
    * [MySQL database installation](#mysqlinstallation)
    * [Unit tests](#unittests)
* [Configuration](#configuration)
* [Running](#running)
* [Usage](#usage)
* [Contact](#contact)

##<a name="whatis"></a>What is tidoop-mr-lib-api
tidoop-mr-lib-api exposes a RESTful API for [tidoop-mr-lib](../tidoop-mr-lib). This means for each ready to use MapReduce job available in the library there is an equivalent REST method in the API (of course, using the same parameters) in charge of running the job.

Please observe the MapReduce jobs usually take some time to return a result. This is why the tidoop-mr-lib-api operations run the job, but do not return any result, except a `200 OK` if the job could be run and a job identifier. In order to get the result of the opearion (or its progress), such a job identifier must be used for querying the API later.

[Top](#top)

##<a name="maininstall"></a>Installation
This is a software written in JavaScript, specifically suited for [Node.js](https://nodejs.org) (<i>JavaScript on the server side</i>). JavaScript is an interpreted programming language thus it is not necessary to compile it nor build any package; having the source code downloaded somewhere in your machine is enough.

###<a name="prerequisites"></a>Prerequisites
This REST API has no sense if tidoop-mr-lib is not installed. And tidoop-mr-lib has only sense in a [Hadoop](http://hadoop.apache.org/) cluster, thus both the library and Hadoop are required.

As said, tidoop-mr-lib-api is a Node.js application, therefore install it from the official [download](https://nodejs.org/download/). An advanced alternative is to install [Node Version Manager](https://github.com/creationix/nvm) (nvm) by creationix/Tim Caswell, whcih will allow you to have several versions of Node.js and switch among them.

Launched MapReduce jobs are tracked by means of a MySQL database, thus a MySQL server must be installed somewhere of your choice and be accessible by the API.

Of course, common tools such as `git` and `curl` are needed.

[Top](#top)

###<a name="apiinstallation"></a>API installation
Start by cloning the Tidoop repository somewhere of your ownership:

    $ git clone https://github.com/telefonicaid/fiware-tidoop.git
    
tidoop-mr-lib-api code is located at `fiware-tidoop/tidoop-mr-lib-api`. Change to that directory and execute the installation command:

    $ cd fiware-tidoop/tidoop-mr-lib-api
    $ npm install
    
That must download all the dependencies under a `node_modules` directory.

[Top](#top)

###<a name="mysqlinstallation"></a>MySQL database installation
Use the file `resources/mysql_db_and_tables.sql` for creating both `cosmos` database (if not yet existing) and `tidoop_job` table.

    $ mysql -u <mysql_user> -p < resources/mysql_db_and_tables.sql

The `tidoop_job` table tracks, for each MapReduce job:

* The job id, with format `tidoop_job_<timestamp>`.
* The job type, e.g. `map_only` or `filter`.
* The timestamp when the job was launched.
* The timestamp when the job finished.
* The mapping progress, in percentage.
* The reducing progress, in percentage.

[Top](#top)

###<a name="unittests"></a>Unit tests
To be done.

[Top](#top)

##<a name="configuration"></a>Configuration
tioop-mr-lib-api is configured through a JSON file (`conf/tidoop-mr-lib-api.json`). These are the available parameters:

* **host**: FQDN or IP address of the host running the service. Do not use `localhost` unless you want only local clients may access the service.
* **port**: TCP listening port for incomming API methods invocation. 12000 by default.
* **tidoopMRLibPath**: Installation path for tidoop-mr-lib.
* **mysql**:
    * **host**: FQDN or IP address of the host running the service.
    * **port**: TCP listening port for the MySQL service, typically 3306.
    * **user**: A valid user allowed to write and read the MySQL database.
    * **password**: Password for above user.
    * **database**: Database used to track information regarding the launched MR jobs; "cosmos" by default.

[Top](#top)

##<a name="running"></a>Running
The Http server implemented by tidoop-mr-lib-api is run as (assuming your current directory is `fiware-tidoop/tidoop-mr-lib-api`):

    $ npm start
    
If everything goes well, you should be able to remotely ask (using a web browser or `curl` tool) for the version of the software:

    $ curl -X GET "http://<host_running_the_api>:12000/version"
    {version: 0.0.0}
    
tidoop-mr-lib-api typically listens in the TCP/12000 port, but you can change if by editing `conf/tidoop-mr-lib-api.conf` as seen above.

[Top](#top)

##<a name="usage"></a>Usage
Please refer to this [Apiary](http://docs.tidoopmrlibapi.apiary.io/#) documentation.

[Top](#top)

##<a name="contact"></a>Reporting issues and contact information
There are several channels suited for reporting issues and asking for doubts in general. Each one depends on the nature of the question:

* Use [stackoverflow.com](http://stackoverflow.com) for specific questions about the software. Typically, these will be related to installation problems, errors and bugs. Development questions when forking the code are welcome as well. Use the `fiware-tidoop` tag.
* Use [fiware-tech-help@lists.fi-ware.org](mailto:fiware-tech-help@lists.fi-ware.org) for general questions about the software. Typically, these will be related to the conceptual usage of the component, e.g. wether it suites for your project or not. It is worth to mention the issues reported to [fiware-tech-help@lists.fi-ware.org](mailto:fiware-tech-help@lists.fi-ware.org) are tracked under [http://jira.fiware.org](http://jira.fiware.org); use this Jira to see the status of the issue, who has been assigneed to, the exchanged emails, etc, nevertheless the answers will be sent to you via email too.
* Personal email:
    * [francisco.romerobueno@telefonica.com](mailto:francisco.romerobueno@telefonica.com) **[Main contributor]**
    * [german.torodelvalle@telefonica.com](german.torodelvalle@telefonica.com) **[Contributor]**

**NOTE**: Please try to avoid personaly emailing the contributors unless they ask for it. In fact, if you send a private email you will probably receive an automatic response enforcing you to use [stackoverflow.com](stackoverflow.com) or [fiware-tech-help@lists.fi-ware.org](mailto:fiware-tech-help@lists.fi-ware.org). This is because using the mentioned methods will create a public database of knowledge that can be useful for future users; private email is just private and cannot be shared.

[Top](#top)
