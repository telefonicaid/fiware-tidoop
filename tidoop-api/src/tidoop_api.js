/**
 * Copyright 2016 Telefonica Investigación y Desarrollo, S.A.U
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
 * Http server for Tidoop REST API
 *
 * Author: frb
 */

// Module dependencies
var Hapi = require('hapi');
var boom = require('boom');
var cmdRunner = require('./cmd_runner.js');
var packageJson = require('../package.json');
var config = require('../conf/tidoop-api.json');
var mysqlDriver = require('./mysql_driver.js');
var serverUtils = require('./server_utils.js');
var logger = require('./logger.js');

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
        logger.info("Request: GET /tidoop/v1/version");
        var response = '{version: ' + packageJson.version + '}';
        logger.info("Response: " + response);
        reply(response);
    } // handler
});

server.route({
    method: 'GET',
    path: '/tidoop/v1/user/{userId}/jobs',
    handler: function(request, reply) {
        logger.info('Request: GET /tidoop/v1/user/' + request.params.userId + '/jobs/');
        reply(boom.notImplemented('Unsupported operation'));
    } // handler
});

server.route({
    method: 'POST',
    path: '/tidoop/v1/user/{userId}/{inputDataPath}',
    handler: function (request, reply) {
        var userId = request.params.userId;
        var inputData = '/user/' + userId + '/' + request.params.inputDataPath;
        var jarPath = request.payload.jar_path;
        var className = request.payload.class_name;
        var jobId = 'tidoop_job_' + Date.now();
        var outputData = '/user/' + userId + '/jobs/' + jobId + '/output';

        logger.info('Request: POST /tidoop/v1' + inputData + ' ' + JSON.stringify(request.payload));

        // Create a new job entry in the database
        mysqlDriver.addJob(jobId, 'custom', function(error, result) {
            if (error) {
                logger.error('The new job could not be added to the database');
                reply(boom.internal('The new job could not be added to the database', error));
            } else {
                // Run the job; the callback function will receive the complete output once it finishes
                cmdRunner.run(jobId, 'hadoop', ['jar', jarPath, className, '-libjars', jarPath, inputData, outputData]),
                    function(error, result) {
                        if (error) {
                            logger.error('The MR job could not be run');
                            reply(boom.internal('The MR job could not be run', error));
                        } else {
                            logger.info(result);
                        } // if else
                    }
                );

                // Create the response
                var response = '{job_id: ' + jobId + '}';
                logger.info("Response: " + response);

                // Return the response
                reply(response);
            }
        });
    } // handler
});

server.route({
    method: 'GET',
    path: '/tidoop/v1/user/{userId}/jobs/{jobId}',
    handler: function (request, reply) {
        var userId = request.params.userId;
        var jobId = request.params.jobId;

        logger.info('Request: GET /tidoop/v1/user/' + userId + '/jobs/' + jobId);

        // Get the job status
        var result = mysqlDriver.getJob(jobId, function (error, result) {
            if (error) {
                logger.error('Could not get job information for the given job_id');
                reply(boom.internal('Could not get job information for the given job_id', error));
            } else if (result[0]) {
                // Create the response
                var response = '{job_id: ' + jobId + ', job_type: ' + result[0].jobType + ', start_time: ' +
                    result[0].startTime + ', end_time: ' + result[0].endTime + ', map_progress: ' +
                    result[0].mapProgress + ', reduce_progress: ' + result[0].reduceProgress + '}';
                logger.info("Response: " + response);

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
    path: '/tidoop/v1/user/{userId}/jobs/{jobId}',
    handler: function(request, reply) {
        var userId = request.params.userId;
        var jobId = request.params.jobId;

        logger.info('Request: DELETE /tidoop/v1/user/' + userId + '/jobs/' + jobId);

        reply(boom.notImplemented('Unsupported operation'));
    } // handler
});

// Create a connection to the MySQL server, and start the Hapi server
mysqlDriver.connect(function(error) {
    if (error) {
        logger.error('There was some error when connecting to MySQL database. The server will not be run. ' +
            'Details: ' + error);
    } else {
        // Start the Hapi server
        server.start(function(error) {
            if(error) {
                logger.error("Some error occurred during the starting of the Hapi server. Details: " + error);
            } else {
                logger.info("tidoop-mr-lib-api running at http://" + config.host + ":" + config.port);
            } // if else
        });
    } // if else
});
