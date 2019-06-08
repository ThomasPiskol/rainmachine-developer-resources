from RMParserFramework.rmParser import RMParser  # Mandatory include for parser definition
from RMUtilsFramework.rmLogging import log       # Optional include for logging

import json    # Your parser needed libraries
import urllib2, StringIO, datetime, re, csv
import xml.etree.ElementTree as ET
from zipfile import ZipFile
from RMUtilsFramework.rmTimeUtils import rmTimestampFromDateAsString, rmDeltaDayFromTimestamp, rmCurrentDayTimestamp


def parseString(stringValue):
    try:
        value = float(stringValue)
    except ValueError:
        value = None
    if value != None and value < 0:
        value = None
    return value



class DWDHistory(RMParser):
    parserName = "DWD History Parser"  # Your parser name
    parserDescription = "Parser for german DWD KMZ files from opendata.dwd.de. To get the station ID see https://opendata.dwd.de/climate/observations_germany/climate/hourly/precipitation/recent/RR_Stundenwerte_Beschreibung_Stationen.txt" # A short description of your parser
    parserForecast = False # True if parser provides future forecast data
    parserHistorical = True # True if parser also provides historical data (only actual observed data)
    parserInterval = 30 * 60  # every 30min           # Your parser running interval in seconds, data will only be mixed in hourly intervals
    parserDebug = True
    params = {"StationID": None}
    defaultParams = {"StationID": "02860"}

    def perform(self):                # The function that will be executed must have this name

        # Accessing system location settings
        #lat = self.settings.location.latitude
        log.info("Hello History")
        # Other location settings
        #self.zip
        #self.name
        #self.state
        #self.latitude
        #self.longitude
        #self.address
        #self.elevation
        #self.gmtOffset
        #self.dstOffset
        #self.stationID
        #self.stationName
        #self.et0Average

        station = self.params.get("StationID", None)
        if station is None or station == "":
            station = "02860"
            log.debug("No station set, using (%s)" % station)

        #url = "https://opendata.dwd.de/climate/observations_germany/climate/hourly/precipitation/recent/stundenwerte_RR_" + str(station) + "_akt.zip"
        url = "https://opendata.dwd.de/climate/observations_germany/climate/1_minute/precipitation/now/1minutenwerte_nieder_" + str(station) + "_now.zip"


        URLParams = [
            ("User-Agent", "RainMachine v2")
        ]

        try:
            req = urllib2.Request(url)
            response = urllib2.urlopen(req)
            raw = response.read()

            zipFile = ZipFile(StringIO.StringIO(raw))
            dataFile = None
            for fileInfo in zipFile.filelist:
                if fileInfo.filename.startswith("produkt_ein_now_"):
                    dataFile = fileInfo

            if dataFile == None:
                log.error("Unable to find data file.")
                return

            content = zipFile.read(dataFile)
            reader = csv.reader(StringIO.StringIO(content), delimiter=';')
            next(reader)
            #minDate = datetime.datetime.today() - datetime.timedelta(days=30)
            minDate = rmDeltaDayFromTimestamp(rmCurrentDayTimestamp(), -30)
            epochDate = datetime.datetime(1970,1,1)
            log.debug("minDate: %s" % minDate)
            lastHour = None
            currentHour = None
            totalHour = 0
            for row in reader:
                timeStamp = row[1]
                #log.debug("Timestamp: %s" % timeStamp)
                myDate = datetime.datetime.strptime(row[1], "%Y%m%d%H%M")
                myEpoch = (int)((myDate - epochDate).total_seconds())

                #date = rmTimestampFromDateAsString(timeStamp, "%Y%m%d%H")
                if myEpoch is None:
                    log.debug("Cannot convert timestamp: %s to unix timestamp" % timeStamp)
                    continue
                if myEpoch < minDate:
                    continue
                value = parseString(row[3])
                if value == None:
                    continue

                currentHour = myEpoch - (myEpoch % 3600)
                if currentHour != lastHour:
                    if lastHour != None:
                        # log.debug("Adding value %s" % value)
                        self.addValue(RMParser.dataType.RAIN, lastHour, totalHour)
                    totalHour = value
                    lastHour = currentHour
                else:
                    totalHour += value


            log.info("Done")


        except Exception, e:
            log.error("*** Error running DWD parser")
            log.exception(e)

        # downloading data from a URL convenience function since other python libraries can be used
        # data = self.openURL(URL STRING, PARAMETER LIST)
        # URL = "https://example.com/
        # parameterList = [ ("parameter1", "value"),("parameter2", "value") ]


        # After parsing your data you can add it into a database automatically created for your parser with
        # self.addValue( VALUE TYPE, UNIX TIMESTAMP, VALUE)
        # Adding multiple values at once is possible with
        # self.addValues( VALUE TYPE, LIST OF TUPLES [ (TIMESTAMP, VALUE), (TIMESTAMP, VALUE) ... ]
        # Predefined VALUE TYPES
        # RMParser.dataType.TEMPERATURE
        # RMParser.dataType.MINTEMP
        # RMParser.dataType.MAXTEMP
        # RMParser.dataType.RH
        # RMParser.dataType.WIND
        # RMParser.dataType.SOLARRADIATION
        # RMParser.dataType.SKYCOVER
        # RMParser.dataType.RAIN
        # RMParser.dataType.ET0
        # RMParser.dataType.POP
        # RMParser.dataType.QPF
        # RMParser.dataType.CONDITION
        # RMParser.dataType.PRESSURE
        # RMParser.dataType.DEWPOINT


        # For your own custom values you can use
        # self.addUserValue( YOUR CUSTOM VALUE NAME, TIMESTAMP, VALUE)



if __name__ == "__main__":
    p = DWDHistory()
    p.perform()