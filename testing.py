from healthandstatus.mongodb import CustomMongodbDriver, CustomMongodbFileProcessor
import time

def main():

    # Create database object
    db = CustomMongodbDriver(username="admin", password="admin")
    # Sets the active database
    db.setDatabase("HealthAndStatus")

    # Object to recursively add individual files to the database
    dbFileProcessor = CustomMongodbFileProcessor(driver=db)
    # Handles #Tablename found in H&S headers
    dbFileProcessor.setIgnoreFirstHeader(False)
    # Data will not be written until the batch size or end of file is reached
    dbFileProcessor.setBatchSize(1000000)
    
    #Search for files and add them to the database
    #traits list means only process filenames containing a matched trait (used to ignore .sqlite for now)
    start_time = time.time()
    results = dbFileProcessor.processFolder(src="/tmp/healthandstatus-mongo-testing/files", splitchar="_", sep=",", traits=[".csv"])
    print(results)
    print("Elapsed time: {0}".format(time.time() - start_time))

    # Query 5,000,000 documents in database
    #query_results = db.read(collection="Hyperic", data={})

    #print(query_results[0])

if __name__ == "__main__":
    main()