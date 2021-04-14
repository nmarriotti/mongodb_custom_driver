from healthandstatus.mongodb import CustomMongodbDriver, CustomMongodbFileProcessor

def main():

    # Create database object
    db = CustomMongodbDriver(username="admin", password="admin")
    # Sets the active database
    db.setDatabase("HealthAndStatus")

    # Object to recursively add individual files to the database
    dbFileProcessor = CustomMongodbFileProcessor(driver=db)
    
    # Handles #Tablename found in H&S headers
    dbFileProcessor.setIgnoreFirstHeader(True)
    
    # Data will not be written until the batch size or end of file is reached
    dbFileProcessor.setBatchSize(1000000)
    
    # Overwriting will attempt to reingest previously ingested files
    dbFileProcessor.setOverwrite(False)
    
    #Search for files and add them to the database
    #traits list means only process filenames containing a matched trait (used to ignore .sqlite for now)
    dbFileProcessor.processFolder(src="/tmp/files", splitchar="_", sep=",", traits=[".dat"])
    

if __name__ == "__main__":
    main()
