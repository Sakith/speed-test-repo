package com.sysco.speed.architecture.schemaregistry.utill;

import com.hortonworks.registries.schemaregistry.*;
import com.sysco.speed.architecture.exceptions.SchemaException;
import com.sysco.speed.architecture.service.schemaregistry.SchemaRegistryDefaultConfigs;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.Collection;
import java.util.Map;
import java.util.HashMap;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

import static org.apache.http.protocol.HTTP.USER_AGENT;

/**
 * Created by charithap on 9/20/17.
 */
public class SyscoSchemaRegistryClient {
    private static final Logger LOGGER = LoggerFactory.getLogger(SyscoSchemaRegistryClient.class);
    private SchemaRegistryDefaultConfigs configurations;

    public SyscoSchemaRegistryClient(SchemaRegistryDefaultConfigs configs) {
        configurations = configs;
    }

    public String getSchemaBySchemaVersionId(String id) {
        String url = configurations.getSchemaRegistryApiUrl() + "/confluent/schemas/ids/" + id;

        try {
            URL obj = new URL(url);
            HttpURLConnection con = (HttpURLConnection) obj.openConnection();

            // optional default is GET
            con.setRequestMethod("GET");

            //add request header
            con.setRequestProperty("User-Agent", USER_AGENT);

            int responseCode = con.getResponseCode();
            LOGGER.info("\nSending 'GET' request to URL : " + url);
            LOGGER.info("Response Code : " + responseCode);

            BufferedReader in = new BufferedReader(
                    new InputStreamReader(con.getInputStream()));
            String inputLine;
            StringBuilder response = new StringBuilder();

            while ((inputLine = in.readLine()) != null) {
                response.append(inputLine);
            }
            in.close();

            JSONObject json = new JSONObject(response.toString());
            return json.getString("schema");

        } catch (MalformedURLException e) {
            LOGGER.error("[SyscoSchemaRegistryClient] Malformed URL Exception: " + e.getMessage());
            throw new SchemaException(e.getMessage());
        } catch (IOException e) {
            LOGGER.error("[SyscoSchemaRegistryClient] IO Exception: " + e.getMessage());
            throw new SchemaException(e.getMessage());
        } catch (Exception e) {
            LOGGER.error("[SyscoSchemaRegistryClient] Exception: " + e.getMessage());
            throw new SchemaException(e.getMessage());
        }
    }

    public String getSchemaVersionId (String schemaName, Integer version) {
        String url = configurations.getSchemaRegistryApiUrl() + "/schemaregistry/schemas/"+schemaName+"/versions/"+version;

        try {
            URL obj = new URL(url);
            HttpURLConnection con = (HttpURLConnection) obj.openConnection();

            // optional default is GET
            con.setRequestMethod("GET");

            //add request header
            con.setRequestProperty("User-Agent", USER_AGENT);

            int responseCode = con.getResponseCode();
            LOGGER.info("\nSending 'GET' request to URL : " + url);
            LOGGER.info("Response Code : " + responseCode);

            BufferedReader in = new BufferedReader(
                    new InputStreamReader(con.getInputStream()));
            String inputLine;
            StringBuilder response = new StringBuilder();

            while ((inputLine = in.readLine()) != null) {
                response.append(inputLine);
            }
            in.close();

            JSONObject json = new JSONObject(response.toString());

            return Integer.toString(json.getInt("id"));

        } catch (MalformedURLException e) {
            LOGGER.error("[SyscoSchemaRegistryClient] Malformed URL Exception: " + e.getMessage());
            throw new SchemaException(e.getMessage());
        } catch (IOException e) {
            LOGGER.error("[SyscoSchemaRegistryClient] IO Exception: " + e.getMessage());
            throw new SchemaException(e.getMessage());
        } catch (Exception e) {
            LOGGER.error("[SyscoSchemaRegistryClient] Exception: " + e.getMessage());
            throw new SchemaException(e.getMessage());
        }

    }

    public JSONObject getAllSchemaVersions (String schemaName) {
        String url = configurations.getSchemaRegistryApiUrl() + "/schemaregistry/schemas/"+schemaName+"/versions";

        try {
            URL obj = new URL(url);
            HttpURLConnection con = (HttpURLConnection) obj.openConnection();

            // optional default is GET
            con.setRequestMethod("GET");

            //add request header
            con.setRequestProperty("User-Agent", USER_AGENT);

            int responseCode = con.getResponseCode();
            LOGGER.info("\nSending 'GET' request to URL : " + url);
            LOGGER.info("Response Code : " + responseCode);

            BufferedReader in = new BufferedReader(
                    new InputStreamReader(con.getInputStream()));
            String inputLine;
            StringBuilder response = new StringBuilder();

            while ((inputLine = in.readLine()) != null) {
                response.append(inputLine);
            }
            in.close();

            JSONObject json = new JSONObject(response.toString());
            return json;

        } catch (MalformedURLException e) {
            LOGGER.error("[SyscoSchemaRegistryClient] Malformed URL Exception: " + e.getMessage());
            throw new SchemaException(e.getMessage());
        } catch (IOException e) {
            LOGGER.error("[SyscoSchemaRegistryClient] IO Exception: " + e.getMessage());
            throw new SchemaException(e.getMessage());
        } catch (Exception e) {
            LOGGER.error("[SyscoSchemaRegistryClient] Exception: " + e.getMessage());
            throw new SchemaException(e.getMessage());
        }

    }

    //TODO: USE REST API FOR REMAINING METHODS
    //Methods using the SchemaRegistry client have been commented out

//    public Collection<SerDesInfo> getSchemaBindedSerDes(String schemaName) {
//        return schemaRegistryClient.getSerDes(schemaName);
//    }

//    public Long registerSchemaWithJsonPayload(SchemaMetadata schemaMetadata, String jsonPayload) {
//        // Create schema from json payload.
//        try {
//            String schema = new AvroConverter(new ObjectMapper()).convert(jsonPayload);
//            return registerSchema(schemaMetadata, schema);
//        } catch (IOException e) {
//            LOGGER.error("[SyscoSchemaRegistryClient] IO Exception: " + e.getMessage());
//            throw new SchemaException(e.getMessage());
//        }
//    }

//    public Long registerSchema(SchemaMetadata schemaMetadata, String schema) {
//
//
//        try {
//            Long schemaId = schemaRegistryClient.addSchemaMetadata(schemaMetadata);
//
//            SchemaMetadataInfo tempSchemaMetadata = schemaRegistryClient.getSchemaMetadataInfo(schemaId);
//            SchemaVersion version = new SchemaVersion(schema, tempSchemaMetadata.getSchemaMetadata().getDescription());
//            schemaRegistryClient.addSchemaVersion(tempSchemaMetadata.getSchemaMetadata(), version);
//
//            return schemaId;
//
//        }
//        catch (InvalidSchemaException e) {
//            LOGGER.error("[SyscoSchemaRegistryClient] Exception: " + e.getMessage());
//            throw new SchemaException(e.getMessage());
//        } catch (IncompatibleSchemaException e) {
//            LOGGER.error("[SyscoSchemaRegistryClient] Exception: " + e.getMessage());
//            throw new SchemaException(e.getMessage());
//        } catch (SchemaNotFoundException e) {
//            LOGGER.error("[SyscoSchemaRegistryClient] Exception: " + e.getMessage());
//            throw new SchemaException(e.getMessage());
//        }
//        catch (Exception e) {
//            LOGGER.error("[SyscoSchemaRegistryClient] Exception: " + e.getMessage());
//            throw new SchemaException(e.getMessage());
//        }
//
//
//    }

}
