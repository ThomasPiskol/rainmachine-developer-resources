from RMParserFramework.rmParser import RMParser  # Mandatory include for parser definition
from RMUtilsFramework.rmLogging import log       # Optional include for logging

import json    # Your parser needed libraries
import urllib2, StringIO, datetime, re
import xml.etree.ElementTree as ET
from zipfile import ZipFile
from RMUtilsFramework.rmTimeUtils import rmTimestampFromDateAsString




class DWDData:
    Temperature = None
    MinTemp = None
    MaxTemp = None
    RH = None
    Wind = None
    SolarRadiation = None
    SkyCover = None
    #Rain = None
    ET0 = None
    POP = None
    QPF = None
    Condition = None
    Pressure = None
    DewPoint = None
    Condition = None

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

def temperatureTransformation(value):
    return value - 273.15

def pressureTransformation(value):
    return value / 1000

def skyCoverTransform(value):
    return value / 100

def yesterday(timeStamp):
    return timeStamp - (24 * 60 * 60)

def parseFloats(rawValues, timeStamps, transform = None, timeTransform = None):
    floatList = []
    if len(timeStamps) != len(rawValues):
        log.error("Mismatch between timestamps and values.")
        return floatList
    i = 0
    for rawValue in rawValues:
        try:
            value = float(rawValue)
        except ValueError:
            i += 1
            continue
        if transform != None:
            value = transform(value)
        ts = timeStamps[i]
        if timeTransform != None:
            ts = timeTransform(ts)
        tuple = [ts, value]
        floatList.append(tuple)
        i += 1
    return floatList


def conditionParser(dwdValue):
    if dwdValue == 95:
        return RMParser.conditionType.Thunderstorm
    if dwdValue == 57:
        return RMParser.conditionType.HeavyFreezingRain
    if dwdValue == 56:
        return RMParser.conditionType.FreezingRain
    if dwdValue == 67:
        return RMParser.conditionType.RainIce
    if dwdValue == 66:
        return RMParser.conditionType.RainIce
    if dwdValue == 86 or dwdValue == 85:
        return RMParser.conditionType.Snow
    if dwdValue == 84 or dwdValue == 83:
        return RMParser.conditionType.RainShowers
    if dwdValue == 82:
        return RMParser.conditionType.HeavyRain
    if dwdValue == 81:
        return RMParser.conditionType.RainShowers
    if dwdValue == 80:
        return RMParser.conditionType.LightRain
    if dwdValue == 75 or dwdValue == 73 or dwdValue == 72 or dwdValue == 71:
        return RMParser.conditionType.Snow
    if dwdValue == 69 or dwdValue == 68:
        return RMParser.conditionType.RainSnow
    if dwdValue == 55 or dwdValue == 65:
        return RMParser.conditionType.HeavyRain
    if dwdValue == 53 or dwdValue == 63:
        return RMParser.conditionType.RainShowers
    if dwdValue == 51 or dwdValue == 61:
        return RMParser.conditionType.LightRain
    if dwdValue == 49 or dwdValue == 45:
        return RMParser.conditionType.Fog
    if dwdValue == 3:
        return RMParser.conditionType.Overcast
    if dwdValue == 2:
        return RMParser.conditionType.MostlyCloudy
    if dwdValue == 1:
        return RMParser.conditionType.FewClouds
    if dwdValue == 0:
        return RMParser.conditionType.Fair
    return RMParser.conditionType.Unknown


class DWDForecast(RMParser):
    parserName = "DWD Forecast Parser"  # Your parser name
    parserDescription = "Parser for german DWD KMZ files from opendata.dwd.de. To get the station ID see https://www.dwd.de/DE/leistungen/opendata/help/stationen/mosmix_stationskatalog.cfg?view=nasPublication&nn=16102" # A short description of your parser
    parserForecast = True # True if parser provides future forecast data
    parserHistorical = False # True if parser also provides historical data (only actual observed data)
    parserInterval = 1 * 3600             # Your parser running interval in seconds, data will only be mixed in hourly intervals
    parserDebug = True
    params = {"station": None}
    defaultParams = {"station": "K4086"}

    def perform(self):                # The function that will be executed must have this name

        # Accessing system location settings
        #lat = self.settings.location.latitude
        log.info("Hello KMZ")
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

        station = self.params.get("station", None)
        if station is None or station == "":
            station = "K4086"
            log.debug("No station set, using (%s)" % station)

        url = "https://opendata.dwd.de/weather/local_forecasts/mos/MOSMIX_L/single_stations/" + str(
            station) + "/kml/MOSMIX_L_LATEST_" + str(station) + ".kmz"

        URLParams = [
            ("User-Agent", "RainMachine v2")
        ]

        try:
            req = urllib2.Request(url)
            response = urllib2.urlopen(req)
            raw = response.read()

            zipFile = ZipFile(StringIO.StringIO(raw))
            kml = zipFile.read(zipFile.filelist[0])

            rootNode = ET.fromstring(kml)

            nameSpaces = {
                'dwd': "https://opendata.dwd.de/weather/lib/pointforecast_dwd_extension_V1_0.xsd",
                'gx': "http://www.google.com/kml/ext/2.2",
                'xal': "urn:oasis:names:tc:ciq:xsdschema:xAL:2.0",
                'kml': "http://www.opengis.net/kml/2.2",
                'atom': "http://www.w3.org/2005/Atom"
            }

            timeStampsNode = rootNode.findall("./kml:Document/kml:ExtendedData/dwd:ProductDefinition/dwd:ForecastTimeSteps/", nameSpaces)
            extendedDataNode = rootNode.findall("./kml:Document/kml:Placemark/kml:ExtendedData/", nameSpaces)

            # Parse Timestamps
            timeStampList = []
            for ts in timeStampsNode:
                compatibleString = re.sub(r"\.\d+Z$", '', ts.text)
                unix = rmTimestampFromDateAsString(compatibleString, "%Y-%m-%dT%H:%M:%S")
                #ts = datetime.datetime.strptime(compatibleString, "%Y-%m-%dT%H:%M:%S")
                timeStampList.append(unix)

            dwdData = []
            parsedData = DWDData()
            for data in extendedDataNode:
                for k,v in data.attrib.items():
                    if k.endswith("elementName"):
                        valueNode = data.find("./dwd:value", nameSpaces)
                        if valueNode == None:
                            continue
                        rawValues = valueNode.text.split()
                        if len(rawValues) != len(timeStampList):
                            continue
                        # Temperature
                        if v.lower() == "TTT".lower():
                            parsedData.Temperature = parseFloats(rawValues, timeStampList, temperatureTransformation)
                            continue
                        # Min Temperature
                        if v.lower() == "TN".lower():
                            parsedData.MinTemp = parseFloats(rawValues, timeStampList, temperatureTransformation)
                            continue
                        # Max Temperature
                        if v.lower() == "TX".lower():
                            parsedData.MaxTemp = parseFloats(rawValues, timeStampList, temperatureTransformation)
                            continue
                        # Probability of precipitation > 0.0mm during the last hour
                        if v.lower() == "wwP".lower():
                            parsedData.POP = parseFloats(rawValues, timeStampList)
                            continue
                        # Wind
                        if v.lower() == "FF".lower():
                            parsedData.Wind = parseFloats(rawValues, timeStampList)
                            continue
                        # Solar Radiation
                        if v.lower() == "Rad1h".lower():
                            parsedData.SolarRadiation = parseFloats(rawValues, timeStampList, pressureTransformation)
                            continue
                        # Cloud
                        if v.lower() == "Neff".lower():
                            parsedData.SkyCover = parseFloats(rawValues, timeStampList, skyCoverTransform)
                            continue
                        # QPF
                        if v.lower() == "RRdc".lower():
                            parsedData.QPF = parseFloats(rawValues, timeStampList, None, yesterday)
                            continue
                        # evapotranspiration
                        if v.lower() == "PEvap".lower():
                            parsedData.ET0 = parseFloats(rawValues, timeStampList, None, yesterday)
                            continue
                        # Pressure
                        if v.lower() == "PPPP".lower():
                            parsedData.Pressure = parseFloats(rawValues, timeStampList, pressureTransformation)
                            continue
                        # Dewpoint
                        if v.lower() == "Td".lower():
                            parsedData.DewPoint = parseFloats(rawValues, timeStampList, temperatureTransformation)
                            continue
                        # Condition
                        if v.lower() == "WPcd1".lower():
                            parsedData.Condition = parseFloats(rawValues, timeStampList, conditionParser, yesterday)
                            continue


            log.info("Adding parsed values to database")
            if parsedData.Temperature != None:
                log.debug("Adding Temparatures values")
                self.addValues(RMParser.dataType.TEMPERATURE, parsedData.Temperature)
            if parsedData.MinTemp != None:
                log.debug("Adding Min-Temparatures values")
                self.addValues(RMParser.dataType.MINTEMP, parsedData.MinTemp)
            if parsedData.MaxTemp != None:
                log.debug("Adding Max-Temparatures values")
                self.addValues(RMParser.dataType.MAXTEMP, parsedData.MaxTemp)
            if parsedData.RH != None:
                log.debug("Adding RH values")
                self.addValues(RMParser.dataType.RH, parsedData.RH)
            if parsedData.Wind != None:
                log.debug("Adding Wind values")
                self.addValues(RMParser.dataType.WIND, parsedData.Wind)
            if parsedData.SolarRadiation != None:
                log.debug("Adding Solar Radiation values")
                self.addValues(RMParser.dataType.SOLARRADIATION, parsedData.SolarRadiation)
            if parsedData.SkyCover != None:
                log.debug("Adding SkyCover values")
                self.addValues(RMParser.dataType.SKYCOVER, parsedData.SkyCover)
            if parsedData.QPF != None:
                log.debug("Adding QPF values")
                self.addValues(RMParser.dataType.QPF, parsedData.QPF)
            if parsedData.ET0 != None:
                log.debug("Adding ET0 values")
                #self.addValues(RMParser.dataType.ET0, parsedData.ET0)
            if parsedData.POP != None:
                log.debug("Adding POP values")
                self.addValues(RMParser.dataType.POP, parsedData.POP)
            if parsedData.Pressure != None:
                log.debug("Adding Pressure values")
                self.addValues(RMParser.dataType.PRESSURE, parsedData.Pressure)
            if parsedData.DewPoint != None:
                log.debug("Adding DewPoint values")
                self.addValues(RMParser.dataType.DEWPOINT, parsedData.DewPoint)
            if parsedData.Condition != None:
                self.addValues(RMParser.dataType.CONDITION, parsedData.Condition)

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
    p = DWDForecast()
    p.perform()