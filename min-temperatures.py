from typing import List
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("MinTemperatures")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    stationID = fields[0]
    entryType = fields[2]
    # temperature = float(fields[3]) * 0.1 * (9.0 / 5.0) + 32.0 -- Fahrenheit
    temperature = float(fields[3]) * 0.1 # -- Celsius
    return (stationID, entryType, temperature)

def min_temps() -> List[str]:
    lines = sc.textFile("1800.csv")
    parsedLines = lines.map(parseLine)
    minTemps = parsedLines.filter(lambda x: "TMIN" in x[1])
    stationTemps = minTemps.map(lambda x: (x[0], x[2]))
    minTemps = stationTemps.reduceByKey(lambda x, y: min(x,y))
    results = minTemps.collect();
    
    formatted_results = [ result[0] + "\t{:.2f}C".format(result[1]) for result in results ]
    return formatted_results

def max_temps() -> List[str]:
    lines = sc.textFile("1800.csv")
    parsedLines = lines.map(parseLine)
    maxTemps = parsedLines.filter(lambda x: "TMAX" in x[1])
    stationTemps = maxTemps.map(lambda x: (x[0], x[2]))
    maxTemps = stationTemps.reduceByKey(lambda x, y: max(x,y))
    results = maxTemps.collect();
    
    formatted_results = [ result[0] + "\t{:.2f}C".format(result[1]) for result in results ]
    return formatted_results

if __name__ == "__main__":
    print("Min Temperatures:")
    for result in min_temps():
        print(result)
        
    print("Max Temperatures:")
    for result in max_temps():
        print(result)
