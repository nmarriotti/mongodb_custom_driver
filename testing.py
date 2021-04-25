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
    
    #Search for files and add them to the database
    #traits list means only process filenames containing a matched trait (used to ignore .sqlite for now)
    dbFileProcessor.start(src="/tmp/files", splitchar="_", sep=",", traits=[".dat"])
    

if __name__ == "__main__":
    main()
