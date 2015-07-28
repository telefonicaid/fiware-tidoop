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

// Module dependencies
var Hapi = require('hapi');
var boom = require('boom');
var cmdRunner = require('./cmd_runner.js');
var packageJson = require('../package.json');
var config = require('../conf/tidoop-mr-lib-api.json');
var mysqlDriver = require('./mysql_driver.js');
var serverUtils = require('./server_utils.js');

// Create a Hapi server with a host and port
var server = new Hapi.Server();

server.connection({ 
    host: config.host,
    port: config.port
});

// Add routes
server.route({
    method: 'GET',
    path: '/tidoop/v1/version',
    handler: function (request, reply) {
        console.log("Request: GET /tidoop/v1/version");
        var response = '{version: ' + packageJson.version + '}';
        console.log("Response: " + response);
        reply(response);
    } // handler
});

server.route({
    method: 'GET',
    path: '/tidoop/v1/jobs/',
    handler: function(request, reply) {
        console.log('Request: GET /tidoop/v1/jobs/');
        reply(boom.notImplemented('Unsupported operation'));
    } // handler
});

server.route({
    method: 'POST',
    path: '/tidoop/v1/jobs/',
    handler: function (request, reply) {
        console.log('Request: POST /tidoop/v1/jobs/ ' + JSON.stringify(request.payload));

        // Check the request parameters
        // TBD

        // Get the jobType
        var jobType = request.payload.job_type;

        // Create a jobId
        var jobId = 'tidoop_job_' + Date.now();

        // Create a new job entry in the database
        mysqlDriver.addJob(jobId, jobType, function(error, result) {
            if (error) {
                reply(boom.internal('The new job could not be added to the database', error));
            } else {
                // Run the Filter MR job; the callback function will receive the complete output once it finishes
                cmdRunner.run(jobId,
                    'hadoop',
                    ['jar', config.tidoopMRLibPath,
                    serverUtils.getMRJobByType(jobType),
                    '-libjars', config.tidoopMRLibPath].concat(
                        serverUtils.getParamsForMRJob(jobType, request.payload)),
                    function(error, result) {
                        if (error) {
                            reply(boom.internal('The MR job could not be run', error));
                        } else {
                            console.log(result);
                        } // if else
                    }
                );

                // Create the response
                var response = '{job_id: ' + jobId + '}';
                console.log("Response: " + response);

                // Return the response
                reply(response);
            }
        });
    } // handler
});

server.route({
    method: 'GET',
    path: '/tidoop/v1/jobs/{jobId}',
    handler: function (request, reply) {
        console.log('Request: GET /tidoop/v1/jobs/' + request.params.jobId + '/');

        // Check the request parameters
        // TBD

        // Get the jobId
        var jobId = request.params.jobId;

        // Get the job status
        var result = mysqlDriver.getJob(jobId, function (error, result) {
            if (error) {
                reply(boom.internal('Could not get job information for the given job_id', error));
            } else if (result[0]) {
                // Create the response
                var response = '{job_id: ' + jobId + ', job_type: ' + result[0].jobType + ', start_time: ' +
                    result[0].startTime + ', end_time: ' + result[0].endTime + ', map_progress: ' +
                    result[0].mapProgress + ', reduce_progress: ' + result[0].reduceProgress + '}';
                console.log("Response: " + response);

                // Return the response
                reply(response);
            } else {
                reply(boom.badRequest('The given job_id=' + jobId + ' does not exist'));
            } // if else
        });
    } // handler
});

server.route({
    method: 'DELETE',
    path: '/tidoop/v1/jobs/{jobId}',
    handler: function(request, reply) {
        console.log('Request: DELETE /tidoop/v1/jobs/' + request.params.jobId + '/');
        reply(boom.notImplemented('Unsupported operation'));
    } // handler
});

// Create a connection to the MySQL server, and start the Hapi server
mysqlDriver.connect(function(error) {
    if (error) {
        console.log('There was some error when connecting to MySQL database. The server will not be run. ' +
            'Details: ' + error);
    } else {
        // Start the Hapi server
        server.start(function(error) {
            if(error) {
                return console.log("Some error occurred during the starting of the Hapi server. Details: " + error);
            } // if

            console.log("tidoop-mr-lib-api running at http://" + config.host + ":" + config.port);
        });
    } // if else
});
