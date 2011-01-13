import time
from java.io import InputStreamReader
from java.io import BufferedReader
from java.util import HashMap
from org.apache.commons.codec.digest import DigestUtils
from au.edu.usq.fascinator.common import JsonConfigHelper

class IndexData:
    def __init__(self):
        pass

    def __activate__(self, context):
        # Prepare variables
        self.index = context["fields"]
        self.object = context["object"]
        self.payload = context["payload"]
        self.params = context["params"]
        self.utils = context["pyUtils"]
        self.indexer = context["indexer"]

        # Common data
        self.__newDoc()

        # Real metadata
        if self.itemType == "object":
            #self.__previews()
            self.__basicData()
            self.__metadata()

        # Make sure security comes after workflows
        self.__security()

    def __newDoc(self):
        self.oid = self.object.getId()
        self.pid = self.payload.getId()
        metadataPid = self.params.getProperty("metaPid", "DC")

        if self.pid == metadataPid:
            self.itemType = "object"
        else:
            self.oid += "/" + self.pid
            self.itemType = "datastream"
            self.utils.add(self.index, "identifier", self.pid)

        self.utils.add(self.index, "id", self.oid)
        self.utils.add(self.index, "storage_id", self.oid)
        self.utils.add(self.index, "item_type", self.itemType)
        self.utils.add(self.index, "last_modified", time.strftime("%Y-%m-%dT%H:%M:%SZ"))
        self.utils.add(self.index, "harvest_config", self.params.getProperty("jsonConfigOid"))
        self.utils.add(self.index, "harvest_rules",  self.params.getProperty("rulesOid"))
        self.utils.add(self.index, "display_type", "geonames")

    def __basicData(self):
        self.utils.add(self.index, "repository_name", self.params["repository.name"])
        self.utils.add(self.index, "repository_type", self.params["repository.type"])

    def __indexList(self, name, values):
        for value in values:
            self.utils.add(self.index, name, value)

    def __security(self):
        roles = self.utils.getRolesWithAccess(self.oid)
        if roles is not None:
            for role in roles:
                self.utils.add(self.index, "security_filter", role)
        else:
            # Default to guest access if Null object returned
            schema = self.utils.getAccessSchema("derby");
            schema.setRecordId(self.oid)
            schema.set("role", "guest")
            self.utils.setAccessSchema(schema, "derby")
            self.utils.add(self.index, "security_filter", "guest")

    def __metadata(self):
        ISOCode = self.params.getProperty("ISOcode")
        print "*** Processing: ", ISOCode
        #Index the country metadata
        metadataPayload = self.object.getPayload("%s.json" % ISOCode)
        json = JsonConfigHelper(metadataPayload.open())
        allMeta = json.getMap(".")
        self.utils.add(self.index, "recordType", "country")
        for key in allMeta.keySet():
            self.utils.add(self.index, key, allMeta.get(key));
        metadataPayload.close()

        #Index the country detail
        geoNamePayload = self.object.getPayload("%s.txt" % ISOCode)
        countryName = self.params.getProperty("countryName")
    
        countryAreaStream = geoNamePayload.open()
        reader = BufferedReader(InputStreamReader(countryAreaStream, "UTF-8"));
        line = reader.readLine()
        
        headerList = ["geonameid", "name", "asciiname", "alternatenames", "latitude", "longitude", \
                      "feature class", "feature code", "country code", "cc2", "admin1 code", "admin2 code", \
                      "admin3 code", "admin4 code", "population", "elevation", "gtopo30", "timezone", "modification date"]
        
        while (line != None):
            arraySplit = line.split("\t")
            geonamesId = arraySplit[0]
            oid = DigestUtils.md5Hex("http://geonames.org/" + geonamesId)

            if oid == self.oid:
                extraIndex = self.index
            else:
                extraIndex = HashMap()
                self.utils.add(extraIndex, "recordType", "area")
                self.utils.add(extraIndex, "item_type", self.itemType)
                self.utils.add(extraIndex, "dc_identifier", oid)
                self.utils.add(extraIndex, "id", oid)
                self.utils.add(extraIndex, "storage_id", self.oid)   #Use parent object
                self.utils.add(extraIndex, "last_modified", time.strftime("%Y-%m-%dT%H:%M:%SZ"))
                self.utils.add(extraIndex, "display_type", "geonames")
                self.utils.add(extraIndex, "countryName", countryName)
                self.utils.add(extraIndex, "repository_name", self.params["repository.name"])
                self.utils.add(extraIndex, "repository_type", self.params["repository.type"])
                self.utils.add(extraIndex, "security_filter", "guest")
            self.utils.add(extraIndex, "dc_title", arraySplit[1])
            # The rest of the metadata
            count = 0
            for array in arraySplit:
                if headerList[count] !="alternatenames" and array:
                    self.utils.add(extraIndex, headerList[count], array)
                count +=1
                
            self.indexer.sendIndexToBuffer(oid, extraIndex)
            line = reader.readLine()
        geoNamePayload.close()
        