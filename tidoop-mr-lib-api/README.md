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
* [Administration](#administration)
    * [Logging traces](#loggingtraces)
    * [Database](#database)
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
Start by creating, if not yet created, a Unix user named `tidoop`; it is needed for installing and running the application. You can only do this as root, or as another sudoer user:

    $ sudo useradd tidoop
    $ sudo passwd tidoop <choose_a_password>
    
While you are a sudoer user, create a folder for saving the tidoop-mr-lib-api log traces under a path of your choice, typically `/var/log/tidoop/tidoop-mr-lib-api`, and set `tidoop` as the owner:

    $ sudo mkdir -p /var/log/tidoop/tidoop-mr-lib-api
    $ sudo chown -R tidoop:tidoop /var/log/tidoop

Now, change to the new fresh `tidoop` user:

    $ su - tidoop

Then, clone the Tidoop repository somewhere of your ownership:

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
* **log**:
    * **file_name**: path of the file where the log traces will be saved in a daily rotation basis. This file must be within the logging folder owned by the the user `tidoop`.
    * **date_pattern**: data pattern to be appended to the log file name when the log file is rotated.

[Top](#top)

##<a name="running"></a>Running
The Http server implemented by tidoop-mr-lib-api is run as (assuming your current directory is `fiware-tidoop/tidoop-mr-lib-api`):

    $ npm start
    
If everything goes well, you should be able to remotely ask (using a web browser or `curl` tool) for the version of the software:

    $ curl -X GET "http://<host_running_the_api>:12000/tidoop/v1/version"
    {version: 0.0.0}
    
tidoop-mr-lib-api typically listens in the TCP/12000 port, but you can change if by editing `conf/tidoop-mr-lib-api.conf` as seen above.

[Top](#top)

##<a name="usage"></a>Usage
Please refer to this [Apiary](http://docs.tidoopmrlibapi.apiary.io/#) documentation.

[Top](#top)

##<a name="administration"></a>Administration
Two are the sources of data, the logs and the database, useful for an administrator of tidoop-mr-lib-api.

[Top](#top)

###<a name="loggingtraces"></a>Logging traces
Logging traces, typically saved under `/var/log/tidoop/tidoop-mr-lib`, are the main source of information regarding the GUI performance. These traces are written in JSON format, having the following fields: level, message and timestamp. For instance:

    {"level":"info","message":"Connected to http://130.206.81.225:3306/cosmos_gui","timestamp":"2015-07-31T08:44:04.624Z"}

Logging levels follow this hierarchy:

    debug < info < warn < error < fatal
    
Within the log it is expected to find many `info` messages, and a few of `warn` or `error` types. Of special interest are the errors:

* ***There was some error when connecting to MySQL database. The server will not be run***: This message may appear when starting the API. Most probably the MySQL endpoint is not correct, the MySQL user is not allowed to remotely connect, of there is some network error like a port filtering.
* ***Some error occurred during the starting of the Hapi server***: This message may appear when starting the API. Most probably the configured host IP address/FQDN does not belongs to the physical machine the service is running, or the configured port is already used.
* ***The new job could not be added to the database***: This message may appear once a MapReduce job has been launched and it is wanted to be tracked throught the database. Most probably the connection with database has been lost, or the MySQL service is unavailable.
* ***The MR job could not be run***: This message may appear when a new MapReduce job from tidoop-mr-lib is wanted to be run. Please check the logs generated by the library since it is not a problem related to the API.
* ***Could not get job information for the given job_id***: This message may appear when the status/progress of a job is requested. Most probably the connection with database has been lost, or the MySQL service is unavailable.

[Top](#top)

###<a name="database"></a>Database

Information regarding launched jobs can be found in a MySQL table named `tidoop_job` within a database named `comsos` in the MySQL deployment you did when installing tidoop-mr-lib-api. Such a table contains the jod ID, the job type, the stating and ending timestamp the progress in terms of map and reduce percentage.

    $ mysql -u cb -p
    Enter password: 
    Welcome to the MySQL monitor.  Commands end with ; or \g.
    ...
    Type 'help;' or '\h' for help. Type '\c' to clear the current input statement.

    mysql> show databases;
    +-----------------------+
    | Database              |
    +-----------------------+
    | information_schema    |
    | cosmos                |
    | mysql                 |
    | test                  |
    +-----------------------+
    4 rows in set (0.00 sec)

    mysql> use cosmos;
    Reading table information for completion of table and column names
    You can turn off this feature to get a quicker startup with -A

    Database changed
    mysql> show tables;
    +------------------+
    | Tables_in_cosmos |
    +------------------+
    | cosmos_user      |
    | tidoop_job       |
    +------------------+
    2 rows in set (0.00 sec)

    mysql> select * from tidoop_job;
    +--------------------------+----------+---------------------+---------------------+-------------+----------------+
    | jobId                    | jobType  | startTime           | endTime             | mapProgress | reduceProgress |
    +--------------------------+----------+---------------------+---------------------+-------------+----------------+
    | tidoop_job_1434361958111 | filter   | 2015-06-15 12:53:24 | 2015-06-15 12:53:46 |         100 |            100 |
    | tidoop_job_1435387171279 | map_only | 2015-06-27 09:40:20 | 0000-00-00 00:00:00 |         100 |              0 |
    +--------------------------+----------+---------------------+---------------------+-------------+----------------+
    30 rows in set (0.00 sec)

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
