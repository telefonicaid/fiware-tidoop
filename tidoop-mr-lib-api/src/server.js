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

// requires
var Hapi = require('hapi');
var config = require('../conf/tidoop-mr-lib-api.json');
var pjson = require('../package.json');

// create a server with a host and port
var server = new Hapi.Server();

server.connection({ 
    host: 'localhost',
    port: config.port
});

// add the route
server.route({
    method: 'GET',
    path:'/version',
    handler: function (request, reply) {
        console.log("Request: GET /version");
        var response = '{version: ' + pjson.version + '}';
        console.log("Response: " + response);
        reply(response);
    }
});

// start the server
server.start(function(err) {
    if(err) {
        return console.log("Some error occurred during the starting of the Hapi server: " + err);
    }

    console.log("tidoop-mr-lib-api running at http://localhost:" + config.port);
});
