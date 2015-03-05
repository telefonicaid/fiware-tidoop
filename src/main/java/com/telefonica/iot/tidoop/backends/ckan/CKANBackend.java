/**
 * Copyright 2015 Telefonica Investigación y Desarrollo, S.A.U
 *
 * This file is part of fiware-connectors (FI-WARE project).
 *
 * fiware-connectors is free software: you can redistribute it and/or modify it under the terms of the GNU Affero
 * General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your
 * option) any later version.
 * fiware-connectors is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the
 * implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License
 * for more details.
 *
 * You should have received a copy of the GNU Affero General Public License along with fiware-connectors. If not, see
 * http://www.gnu.org/licenses/.
 *
 * For those usages not covered by the GNU Affero General Public License please contact with
 * francisco.romerobueno at telefonica dot com
 */

package com.telefonica.iot.tidoop.backends.ckan;

import com.telefonica.iot.tidoop.http.HttpClientFactory;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.log4j.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

/**
 *
 * @author frb
 */
public class CKANBackend {
    
    private final Logger logger;
    private final String ckanHost;
    private final String ckanPort;
    private final String apiKey;
    private final boolean ssl;
    private HttpClientFactory httpClientFactory;
    
    /**
     * Constructor.
     * @param ckanHost
     * @param ckanPort
     * @param ssl
     * @param apiKey
     */
    public CKANBackend(String ckanHost, String ckanPort, boolean ssl, String apiKey) {
        this.ckanHost = ckanHost;
        this.ckanPort = ckanPort;
        this.logger = Logger.getLogger(CKANBackend.class);
        this.apiKey = apiKey;
        this.ssl = ssl;
        this.httpClientFactory = new HttpClientFactory(ssl);
    } // CKANBackend
    
    /**
     * Sets the Http client factory. It is protected since it is only going to be used by the tests.
     * @param httpClientFactory
     */
    protected void setHttpClientFactory(HttpClientFactory httpClientFactory) {
        this.httpClientFactory = httpClientFactory;
    } // setHttpClientFactory
    
    /**
     * Gets the package identifiers/names within a given organization id/name.
     * @param orgId
     * @return The package identifiers/names within the given organization id/name
     */
    public List<String> getPackages(String orgId) {
        logger.debug("Getting the packages within " + orgId + " organization");
        List<String> pkgNames = new ArrayList<String>();

        try {
            String url = "http" + (ssl ? "s" : "") + "://" + ckanHost + ":" + ckanPort
                    + "/api/3/action/organization_show?id=" + orgId;
            CKANResponse resp = doCKANRequest("GET", url, "");
            JSONObject result = (JSONObject) resp.getJsonObject().get("result");
            JSONArray pkgs = (JSONArray) result.get("packages");
            
            if (pkgs == null || pkgs.size() == 0) {
                logger.debug("No packages got from organization " + orgId);
                return pkgNames;
            } // if
            
            for (Object pkg1 : pkgs) {
                JSONObject pkg = (JSONObject) pkg1;
                pkgNames.add((String) pkg.get("id"));
            } // for
            
            return pkgNames;
        } catch (Exception e) {
            logger.debug("An exception occured, returning partial package list. Details = " + e.getMessage());
            return pkgNames;
        } // try catch // try catch
    } // getPackages
    
    /**
     * Gets the resource identifiers/names within a given package id/name.
     * @param pkgId
     * @return The resource identifiers/names within the given package id/name
     */
    public List<String> getResources(String pkgId) {
        logger.debug("Getting the resources within " + pkgId + " + package");
        List<String> resIds = new ArrayList<String>();
        
        try {
            String url = "http" + (ssl ? "s" : "") + "://" + ckanHost + ":" + ckanPort
                    + "/api/3/action/package_show?id=" + pkgId;
            CKANResponse resp = doCKANRequest("GET", url, "");
            JSONObject result = (JSONObject) resp.getJsonObject().get("result");
            JSONArray resources = (JSONArray) result.get("resources");
            
            if (resources == null || resources.size() == 0) {
                logger.debug("No resources got from package " + pkgId);
                return resIds;
            } // if
            
            for (Object res : resources) {
                JSONObject resource = (JSONObject) res;
                resIds.add((String) resource.get("id"));
            } // for
            
            return resIds;
        } catch (Exception e) {
            logger.debug("An exception occured, returning partial resource list. Details=" + e.getMessage());
            return resIds;
        } // try catch
    } // getResources
    
    /**
     * Gets the total number of records within a given resource.
     * @param resId Resource identifier
     * @return The total number of records within the given resource
     */
    public int getNumRecords(String resId) {
        logger.debug("Getting the number of records within " + resId);
        
        try {
            int numRecords = 0;
            int i = 0;
            
            while (true) {
                String url = "http" + (ssl ? "s" : "") + "://" + ckanHost + ":" + ckanPort
                        + "/api/3/action/datastore_search?limit=1000&offset=" + (i * 1000) + "&resource_id=" + resId;
                CKANResponse resp = doCKANRequest("GET", url, "");
                JSONObject result = (JSONObject) resp.getJsonObject().get("result");
                JSONArray records = (JSONArray) result.get("records");
                
                if (records == null || records.size() == 0) {
                    logger.debug("No records got from resource " + resId);
                    break;
                } // if
                
                numRecords += records.size();
                i++;
            } // while
            
            return numRecords;
        } catch (Exception e) {
            logger.debug("An exception occured, returning 0 records. Details=" + e.getMessage());
            return 0;
        } // try catch
    } // resURL
    
    /**
     * Gets the records within a given resource.
     * @param resId
     * @param start
     * @param end
     * @return The records within the given resource
     */
    public JSONArray getRecords(String resId, long start, long end) {
        logger.debug("Getting the [" + start + ", " + end + "] records within " + resId);
        
        try {
            String url = "http" + (ssl ? "s" : "") + "://" + ckanHost + ":" + ckanPort
                    + "/api/3/action/datastore_search?limit=" + end + "&offset=" + start + "&resource_id=" + resId;
            CKANResponse resp = doCKANRequest("GET", url, "");
            JSONObject result = (JSONObject) resp.getJsonObject().get("result");
            return (JSONArray) result.get("records");
        } catch (Exception e) {
            logger.debug("An exception occured, returning empty list of records. Details=" + e.getMessage());
            return new JSONArray();
        } // try catch
    } // getRecords
    
    /**
     * Common method to perform HTTP requests using the CKAN API without payload.
     * @param method HTTP method
     * @param url URL path to be added to the base URL
     * @return CKANResponse associated to the request
     * @throws Exception
     */
    public CKANResponse doCKANRequest(String method, String url) throws Exception {
        return doCKANRequest(method, url, "");
    } // doCKANRequest

    /**
     * Common method to perform HTTP requests using the CKAN API with payload.
     * @param method HTTP method
     * @param url URL path to be added to the base URL
     * @param payload Request payload
     * @return CKANResponse associated to the request
     * @throws Exception
     */
    public CKANResponse doCKANRequest(String method, String url, String payload) throws Exception {
        HttpRequestBase request = null;
        HttpResponse response = null;
        
        try {
            // do the post
            if (method.equals("GET")) {
                request = new HttpGet(url);
            } else if (method.equals("POST")) {
                HttpPost r = new HttpPost(url);

                // payload (optional)
                if (!payload.equals("")) {
                    logger.debug("request payload: " + payload);
                    r.setEntity(new StringEntity(payload, ContentType.create("application/json")));
                } // if
                
                request = r;
            } else {
                throw new Exception("HTTP method not supported: " + method);
            } // if else

            // headers
            request.addHeader("Authorization", apiKey);

            // execute the request
            logger.debug("CKAN operation: " + request.toString());
        } catch (Exception e) {
            throw e;
        } // try catch
        
        try {
            response = httpClientFactory.getHttpClient(ssl).execute(request);
        } catch (Exception e) {
            throw new Exception(e.getMessage());
        } // try catch
        
        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));
            String res = "";
            String line;
            
            while ((line = reader.readLine()) != null) {
                res += line;
            } // while
            
            request.releaseConnection();
            long l = response.getEntity().getContentLength();
            logger.debug("CKAN response (" + l + " bytes): " + response.getStatusLine().toString());

            // get the JSON encapsulated in the response
            logger.debug("response payload: " + res);
            JSONParser j = new JSONParser();
            JSONObject o = (JSONObject) j.parse(res);

            // return result
            return new CKANResponse(o, response.getStatusLine().getStatusCode());
        } catch (IOException e) {
            throw e;
        } catch (IllegalStateException e) {
            throw e;
        } catch (ParseException e) {
            throw e;
        } // try catch
    } // doCKANRequest
    
} // CKANBackend
