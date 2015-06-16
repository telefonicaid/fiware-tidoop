/**
 * Copyright 2015 Telefonica Investigación y Desarrollo, S.A.U
 *
 * This file is part of fiware-tidoop (FI-WARE project).
 *
 * fiware-tidoop is free software: you can redistribute it and/or modify it under the terms of the GNU Affero
 * General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your
 * option) any later version.
 * fiware-tidoop is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the
 * implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License
 * for more details.
 *
 * You should have received a copy of the GNU Affero General Public License along with fiware-tidoop. If not, see
 * http://www.gnu.org/licenses/.
 *
 * For those usages not covered by the GNU Affero General Public License please contact with
 * francisco dot romerobueno at telefonica dot com
 */

/**
 * Http server for "Tidoop MR job library" REST API
 *
 * Author: frb
 */

// module dependencies
var Hapi = require('hapi');
var run = require('./cmd_runner.js').run;
var version = require('../package.json').version;
var port = require('../conf/tidoop-mr-lib-api.json').port;
var tidoopMRLibPath = require('../conf/tidoop-mr-lib-api.json').tidoopMRLibPath;
var tidoopMysql = require('./mysql_driver.js');
var serverUtils = require('./server_utils.js');

// create a server with a host and port
var server = new Hapi.Server();

server.connection({ 
    host: 'localhost',
    port: port
});

// create a connection to the MySQL server
tidoopMysql.connect();

// add routes
server.route({
    method: 'GET',
    path: '/tidoop/v1/version',
    handler: function (request, reply) {
        console.log("Request: GET /tidoop/v1/version");
        var response = '{version: ' + version + '}';
        console.log("Response: " + response);
        reply(response);
    } // handler
});

server.route({
    method: 'GET',
    path: '/tidoop/v1/jobs/',
    handler: function(request, reply) {
        console.log('Request: GET /tidoop/v1/jobs/');
        throw new Error('Unsupported operation');
    } // handler
});

server.route({
    method: 'POST',
    path: '/tidoop/v1/jobs/',
    handler: function (request, reply) {
        console.log('Request: POST /tidoop/v1/jobs/ ' + JSON.stringify(request.payload));

        // check the request parameters
        // TBD

        // get the jobType
        var jobType = request.payload.job_type;

        // create a jobId
        var jobId = 'tidoop_job_' + Date.now();

        // create a new job entry in the database
        tidoopMysql.addNewJob(jobId, jobType, function(error, result) {
            if (error) {
                console.log('The new job could not be added to the database. Details: ' + err);
            } else {
                // run the Filter MR job; the callback function will receive the complete output once it finishes
                run(jobId,
                    'hadoop',
                    ['jar', tidoopMRLibPath,
                    serverUtils.getMRJobByType(jobType),
                    '-libjars', tidoopMRLibPath].concat(
                        serverUtils.getParamsForMRJob(jobType, request.payload)),
                    function(result) {
                        console.log(result);
                    }
                );

                // create the response
                var response = '{job_id: ' + jobId + '}';
                console.log("Response: " + response);

                // return the response
                reply(response);
            }
        });
    } // handler
});

server.route({
    method: 'GET',
    path: '/tidoop/v1/jobs/{jobId?}',
    handler: function (request, reply) {
        console.log('Request: GET /tidoop/v1/jobs/' + request.params.jobId + '/');

        // check the request parameters
        // TBD

        // get the jobId
        var jobId = request.params.jobId;

        // get the job status
        var result = tidoopMysql.getJob(jobId, function (error, result) {
            if (error) {
                throw error;
            } else {
                // create the response
                var response = '{job_id: ' + jobId + ', job_type: ' + result[0].jobType + ', start_time: ' +
                    result[0].startTime + ', end_time: ' + result[0].endTime + ', map_progress: ' +
                    result[0].mapProgress + ', reduce_progress: ' + result[0].reduceProgress + '}';
                console.log("Response: " + response);

                // return the response
                reply(response);
            } // if else
        });
    } // handler
});

server.route({
    method: 'DELETE',
    path: '/tidoop/v1/jobs/{jobId?}',
    handler: function(request, reply) {
        console.log('Request: DELETE /tidoop/v1/jobs/' + request.params.jobId + '/');
        throw new Error('Unsupported operation');
    } // handler
});

// start the server
server.start(function(error) {
    if(error) {
        return console.log("Some error occurred during the starting of the Hapi server. Details: " + error);
    } // if

    console.log("tidoop-mr-lib-api running at http://localhost:" + port);
});
