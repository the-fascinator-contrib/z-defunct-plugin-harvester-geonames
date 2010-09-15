import os
import shutil
import urllib
import zipfile

#Reading file
countryInfoFile = "countryInfo.txt"
downloadFolder = "countryZip"
unzipFolder = "unzipFolder"
deleteFolder = False

#check if countryInfo.txt exist if not request user download from http://download.geonames.org/export/dump/countryInfo.txt
if not os.path.isfile(countryInfoFile):
    raise Exception("countryInfo.txt file is not exist, please download file from http://download.geonames.org/export/dump/countryInfo.txt to this directory")

def deleteDir(dirName):
    for root, dirs, files in os.walk(dirName):
        for f in files:
            os.unlink(os.path.join(root, f))
        for d in dirs:
            shutil.rmtree(os.path.join(root, d))
        os.rmdir(root)

if deleteFolder:
    deleteDir(downloadFolder)
    os.mkdir(downloadFolder)
    deleteDir(unzipFolder)
    os.mkdir(unzipFolder)

f = open(countryInfoFile, 'r')
count = 0
for line in f:
    # Process country
    if not line.startswith("#") and line != "":
        count += 1
        arrayValue = line.split("\t")
        ISOCode = arrayValue[0]
        downloadUrl = "http://download.geonames.org/export/dump/%s.zip" % ISOCode
        destinationFile = "%s/%s.zip" % (downloadFolder, ISOCode)
        if not os.path.isfile(destinationFile):
            #download zipfiles to downloadFolder
            print "*** Downloading: ", downloadUrl
            urllib.urlretrieve(downloadUrl, destinationFile)

            print "  *** Unzipping: %s to: %s" % (destinationFile, unzipFolder)
        
            try:
                zfobj = zipfile.ZipFile(destinationFile)
                for name in zfobj.namelist():
                    if name.endswith('/'):
                        os.mkdir(os.path.join(unzipFolder, name))
                    else:
                        outfile = open(os.path.join(unzipFolder, name), 'wb')
                        outfile.write(zfobj.read(name))
                        outfile.close()
            except Exception, e:
                 print " %s is not a valid zip file" % destinationFile


print 
print "Number of country downloaded: ", count

#remove the readme file
readmeFile = "%s/readme.txt" % unzipFolder
if os.path.isfile(readmeFile):
    os.remove(readmeFile)





