/* 
 * The Fascinator - File System Harvester Plugin
 * Copyright (C) 2009 University of Southern Queensland
 * 
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 */
package au.edu.usq.fascinator.harvester.geonames;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import au.edu.usq.fascinator.api.harvester.HarvesterException;
import au.edu.usq.fascinator.api.storage.DigitalObject;
import au.edu.usq.fascinator.api.storage.Payload;
import au.edu.usq.fascinator.api.storage.Storage;
import au.edu.usq.fascinator.api.storage.StorageException;
import au.edu.usq.fascinator.common.JsonConfigHelper;
import au.edu.usq.fascinator.common.harvester.impl.GenericHarvester;
import au.edu.usq.fascinator.common.storage.StorageUtils;

/**
 * Harvests Geonames Server
 * <p>
 * Configuration options:
 * <ul>
 * <li>countryInfo: country information file</li>
 * <li>baseDir: file to harvest</li>
 * </ul>
 * 
 * @author Linda Octalina
 */
public class GeonamesHarvester extends GenericHarvester {

    /** logging */
    private Logger log = LoggerFactory.getLogger(GeonamesHarvester.class);

    /**
     * List of country code and names
     */
    private Map<String, String> countryInfoMap;

    /**
     * File consists of the country codes and country names
     */
    private File countryInfoFile;

    /** File consists of geoname to be harvested */
    private File geonameFile;

    /**
     * Skos harvester constructor
     */
    public GeonamesHarvester() {
        super("geonames", "Geonames Server Harvester");
    }

    /**
     * Init method to initialise geonames harvester
     */
    @Override
    public void init() throws HarvesterException {
        JsonConfigHelper config;
        // Read config
        try {
            config = new JsonConfigHelper(getJsonConfig().toString());
        } catch (IOException ex) {
            throw new HarvesterException("Failed reading configuration", ex);
        }
        String countryFile = config.get("harvester/geonames/countryInfo");
        if (countryFile != "") {
            countryInfoFile = new File(countryFile);
        } else {
            try {
                countryInfoFile = new File(GeonamesHarvester.class.getResource(
                        "/countryInfo.txt").toURI());
            } catch (URISyntaxException e) {
                throw new HarvesterException("Error getting file URI");
            }
        }
        String baseFile = config.get("harvester/geonames/baseFile", "");
        if (baseFile != "") {
            geonameFile = new File(baseFile);
        } else {
            throw new HarvesterException("No Geoname file specified");
        }
        log.info("setupCountryCode");
        setUpCountryCode();
    }

    /**
     * Setting country code map to be used for harvesting
     * 
     * @throws HarvesterException
     */
    private void setUpCountryCode() throws HarvesterException {
        countryInfoMap = new HashMap<String, String>();
        try {
            BufferedReader input = new BufferedReader(new FileReader(
                    countryInfoFile));
            String line = null;
            try {
                Boolean process = false;
                while ((line = input.readLine()) != null) {
                    if (process) {
                        String splitArray[] = line.split("\t");
                        // Just process the ISO and Country Name
                        countryInfoMap.put(splitArray[0], splitArray[4]);
                    }
                    if (line.startsWith("#ISO\t")) {
                        process = true;
                    }
                }
            } finally {
                input.close();
            }
        } catch (FileNotFoundException e) {
            throw new HarvesterException("Fail to read file:"
                    + countryInfoFile.getName());
        } catch (IOException e) {
            throw new HarvesterException("Fail to close input reader");
        }
    }

    public Map<String, String> getCountryCode() {
        return countryInfoMap;
    }

    /**
     * Harvest the next set of skos concept, and return their Object IDs
     * 
     * @return Set<String> The set of object IDs just harvested
     * @throws HarvesterException If there are errors
     */
    @Override
    public Set<String> getObjectIdList() throws HarvesterException {
        Set<String> geonamesObjectIdList = new HashSet<String>();
        try {
            BufferedReader input = new BufferedReader(new FileReader(
                    geonameFile));
            String line = null;
            try {
                while ((line = input.readLine()) != null) {
                    String splitArray[] = line.split("\t");
                    String objectId = createGeonameObject(splitArray);
                    if (objectId != null)
                        geonamesObjectIdList.add(objectId);
                }
            } catch (StorageException e) {
                throw new HarvesterException("Fail to create object: "
                        + e.getMessage());
            } finally {
                input.close();
            }
        } catch (FileNotFoundException e) {
            throw new HarvesterException("Fail to read file:"
                    + countryInfoFile.getName());
        } catch (IOException e) {
            throw new HarvesterException("Fail to close input reader"
                    + e.getMessage());
        }
        log.info("done");
        return geonamesObjectIdList;
    }

    @Override
    public boolean hasMoreObjects() {
        return false;
    }

    /**
     * Create skos digital object and the payload
     * 
     * @param conceptUri concept to be created
     * @param newConceptUri new uri to be used in fascinator
     * @return created object id
     * @throws HarvesterException if fail to harvest skos concept
     * @throws StorageException if fail to create new object
     * @throws IOException if fail to create new payload
     */
    private String createGeonameObject(String[] geonameDetail)
            throws HarvesterException, StorageException, IOException {

        String geonameId = geonameDetail[0];
        String geonameUrl = "http://geonames.org/" + geonameId;
        String geonameName = geonameDetail[1];
        String latitude = geonameDetail[4];
        String longitude = geonameDetail[5];
        String country = countryInfoMap.get(geonameDetail[8]);

        JsonConfigHelper json = new JsonConfigHelper();
        json.set("dc_identifier", geonameUrl);
        json.set("geonameId", geonameId);
        json.set("country", country);
        json.set("dc_title", geonameName);
        json.set("latitude", latitude);
        json.set("longitude", longitude);

        Storage storage = getStorage();
        log.info("Creating Geoname object: {}", geonameName);
        String oid = DigestUtils.md5Hex(geonameUrl);
        DigitalObject object = StorageUtils.getDigitalObject(storage, oid);
        String pid = "geonames.json";

        Payload payload = StorageUtils.createOrUpdatePayload(object, pid,
                IOUtils.toInputStream(json.toString(), "UTF-8"));
        payload.setContentType("text/json");
        payload.close();

        // update object metadata
        Properties props = object.getMetadata();
        props.setProperty("render-pending", "true");

        object.close();
        return object.getId();
    }

}
