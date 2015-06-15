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
 * MySQL driver.
 *
 * Author: frb
 */

// module dependencies
var mysql = require('mysql');
var host = require('../conf/tidoop-mr-lib-api.json').mysql.host;
var port = require('../conf/tidoop-mr-lib-api.json').mysql.port;
var user = require('../conf/tidoop-mr-lib-api.json').mysql.user;
var password = require('../conf/tidoop-mr-lib-api.json').mysql.password;
var database = require('../conf/tidoop-mr-lib-api.json').mysql.database;

var connection = mysql.createConnection({
    host: host,
    port: port,
    user: user,
    password: password,
    database: database
});

module.exports = {
    connect: function() {
        connection.connect(function (error) {
            if (error) {
                throw error;
            } else {
                console.log('Connected to http://' + host + ':' + port + '/' + database);
                return connection;
            }
        });
    }, // connect

    addNewJob: function (jobId, jobType) {
        var query = connection.query(
            'INSERT INTO tidoop_job (jobId, jobType, startTime, mapProgress, reduceProgress) ' +
            'VALUES (?, ?, NOW(), ?, ?)',
            [jobId, jobType, 0, 0],
            function (error, result) {
                if (error) {
                    throw error;
                } else {
                    console.log('Successful insert: \'INSERT INTO tidoop_job ' +
                        '(jobId, jobType, startTime, mapProgress, reduceProgress) VALUES' +
                        '(' + jobId + ', ' + jobType + ', NOW(), 0, 0)\'');
                } // if else
            }
        );
    }, // addJob

    updateJobStatus: function (jobId, mapProgress, reduceProgress) {
        if (mapProgress == 100 && reduceProgress == 100) {
            var query = connection.query(
                'UPDATE tidoop_job SET endTime=NOW(), mapProgress=?, reduceProgress=? WHERE jobId=\'' + jobId + '\'',
                [mapProgress, reduceProgress],
                function (error, result) {
                    if (error) {
                        throw error;
                    } else {
                        console.log('Successful update: \'UPDATE tidoop_job ' +
                            'SET endTime=NOW(), mapProgress=' + mapProgress + ', reduceProgress=' +
                            reduceProgress + ' WHERE jobId=\'' + jobId + '\'\'');
                    } // if else
                }
            );
        } else {
            var query = connection.query(
                'UPDATE tidoop_job SET mapProgress=?, reduceProgress=? WHERE jobId=\'' + jobId + '\'',
                [mapProgress, reduceProgress],
                function (error, result) {
                    if (error) {
                        throw error;
                    } else {
                        console.log('Successful update: \'UPDATE tidoop_job ' +
                            'SET mapProgress=' + mapProgress + ', reduceProgress=' + reduceProgress +
                            ' WHERE jobId=\'' + jobId + '\'\'');
                    } // if else
                }
            );
        }
    }, // updateJobStatus

    getStatus: function (jobId) {
        var query = connection.query(
            'SELECT mapProgress, reduceProgrss from tidoop_job WHERE jobId=\'' + jobId + '\'',
            function (error, result) {
                if (error) {
                    throw error;
                } else {
                    console.log('Successful select: \'SELECT mapProgrss, reduceProgress from tidoop_job ' +
                        'WHERE jobId=\'' + jobId + '\'\'');
                    return result;
                } // if else
            }
        );
    }, // getStatus

    exists: function (username, password) {
        var query = connection.query(
            'SELECT count(*) FROM user where fiware_username=? and fiware_password=?',
            [username, password],
            function (error, result) {
                if (error) {
                    throw error;
                } else {
                    if (result.length > 0) {
                        console.log("Cosmos user already stored in the database");
                        return true;
                    } else {
                        return false;
                    }
                }
            }
        );
    }, // exists

    close: function(connection) {
        connection.end();
    } // end
}
