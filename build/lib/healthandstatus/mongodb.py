from pymongo import MongoClient
from bson.objectid import ObjectId
import os
import types
import datetime
import hashlib
import sys
import time
import progressbar

class CustomMongodbDriver(object):
    """ CRUD operations """
    
    # Constructor
    def __init__(self, host="127.0.0.1", port=27017, username=None, password=None):
        # Initializing the MongoClient. This helps to 
        # access the MongoDB databases and collections. 
        self.client = MongoClient('{0}:{1}'.format(host, port), username=username, password=password)
        print("Connected to {0} port {1}".format(host, port))


    def setDatabase(self, database):
        self.database = self.client[database]


    def create(self, collection=None, data=None):
        ''' Insert document(s) into a collection '''
        if collection is None:
            raise Exception("Collection not specified")
        else:
            if data is not None:
                try:
                    self.database[collection].insert_many(data, ordered=False, bypass_document_validation=True)  # data should be dictionary
                    return True # success
                except Exception as e:
                    pass
            else:
                raise Exception("Nothing to save, because data parameter is empty")
            # insert failed
            return False


    def read(self, collection=None, data=dict()):
        ''' Query collection in database '''
        if collection is None:
            raise Exception("Collection not specified")
        else:
            try:
                return list(self.database[collection].find(data,{"_id":False})) # list of dicts
            except Exception as e:
                return(str(e))

    
    # update data in collection
    # dict(query) = What is being changed
    # dict(changes) = What the query is being replaced with
    def update(self, collection=None, data=None, changes=None):
        ''' Update data in a collection '''
        if collection is None:
            raise Exception("Collection not specified")
        else:
            if data is not None and changes is not None:
                try:
                    cmd = self.database[collection].update_many(data, { "$set": changes })
                    return cmd # json
                except Exception as e:
                    return(str(e))
            else:
                raise Exception("Query and/or update arguments are empty")


    def createIndex(self, collection=None, index=None):
        if index is None:
            raise Exception("Index not specified")
        if collection is None:
            raise Exception("Collection not specified")
        self.database[collection].create_index(index, unique=True)


    # dict(query) = Key/value pairs describing document(s) to delete
    def delete(self, collection=None, query=None):
        ''' Delete data from a collection '''
        if collection is None:
            raise Exception("Collection not specified")
        else:
            if query is not None:
                try:
                    cmd = self.database[collection].delete_many(query)
                    return cmd.raw_result # json
                except Exception as e:
                    return(str(e))
            else:
                raise Exception("Query argument is empty")


class CustomMongodbFileProcessor():

    def __init__(self, driver):
        self.__db = driver
        self.__ignore_first_header = False
        self.__overwrite = True
        self.__batch_size = 1000
        self.__queue = {}
        self.__results = {
            "files": 0,
            "total_rows": 0,
        }
    
    def setOverwrite(self, value):
        if isinstance(value, bool):
            self.__overwrite = value
        else:
            raise Exception("Overwrite value must be True or False")

    def setBatchSize(self, value):
        try:
            self.__batch_size = int(value)
        except Exception as e:
            print(str(e))


    def setIgnoreFirstHeader(self, value):
        if isinstance(value, bool):
            self.__ignore_first_header = value
        else:
            raise Exception("Value must be True or False")

    def __scanDir(self, src, traits):
        print("Fetching files...")
        filesToProcess = []
        for subdir, dirs, files in os.walk(src):
            for file in files:
                filename = os.path.join(subdir, file)
                approved = True
                for trait in traits:
                    if not trait in filename:
                        approved = False
                if approved:
                    basename = os.path.basename(filename)
                    alreadyProcessed = self.__fileAlreadyProcessed(basename)
                    if not alreadyProcessed or self.__overwrite:
                        filesToProcess.append({"filename": filename, "basename": basename, "alreadyProcessed": alreadyProcessed})
        return filesToProcess
                    


    def processFolder(self, src=None, splitchar="_", sep=",", traits=[]):
        # Create list of files to process
	filesToProcess = self.__scanDir(src, traits)
        numFiles = len(filesToProcess)

        start_time = time.time()
        
        if numFiles > 0:
            print("Processing {0} files...".format(numFiles))
        else:
            print("Nothing to do.")
            return

        bar = progressbar.ProgressBar(maxval=numFiles, \
                                     widgets=[progressbar.Bar('=', '[', ']'), ' ', progressbar.Percentage()])
        bar.start()

        for i in range(0,numFiles):
            filename = filesToProcess[i]["filename"]
            basename = filesToProcess[i]["basename"]
            alreadyProcessed = filesToProcess[i]["alreadyProcessed"]

            if not alreadyProcessed or self.__overwrite:
                self.__parseAndAdd(filename, splitchar, sep)
                self.__results["files"] += 1

                if not alreadyProcessed:
                    self.__db.create(collection="ingestedFiles", data=[{"filename": basename}])
            bar.update(i+1)

        bar.finish()        
        self.__results["seconds_elapsed"] = "%.2f" % (time.time() - start_time)
        
        print("\nTotal files:        {0}".format(self.__results["files"]))
        print("Total rows of data: {0}".format(self.__results["total_rows"]))
        print("Time elapsed:       {0}".format(self.__results["seconds_elapsed"]))

        print("\nFinished.")

    
    def __fileAlreadyProcessed(self, filename):
        results = self.__db.read(collection="ingestedFiles", data={"filename": filename})
        if results:
            return True
        return False


    def __parseAndAdd(self, filename, splitchar, sep):
        # Tally number of files added to database
        try:
            # Get info from filename
            basename = os.path.basename(filename)
            parts = basename.split(splitchar)
            owner = parts[0]
            collection = parts[1]
            system = parts[2]
            filedate = self.__parseDate(os.path.splitext(parts[3])[0])

            if not collection in self.__queue.keys():
                self.__queue[collection] = []
            
            # Create unique index of hash column
            self.__db.createIndex(collection=collection, index=[("hash", 'text')])

            # Read file
            with open(filename, 'r') as file:
                headers = False
                for line in file:
                    
                    line = line.strip("\n")

                    if not line:
                        continue
                    
                    line = line.split(sep)

                    if not headers:
                        # First line contains headers
                        if self.__ignore_first_header:
                            headers = line[1:]
                        else:
                            headers = line
                        
                        #print("Headers: {0}".format(headers))
                        continue
                    

                    data = {headers[i]: line[i].strip() for i in range(0,len(headers))}

                    # Add identifying information
                    data["owner"] = owner
                    data["system"] = system
                    data["date"] = filedate

                    # create string to hash
                    unhashed_string = "{}_{}".format(line, basename).encode('utf-8')
                    # hash line+filename
                    data["hash"] = hashlib.md5(unhashed_string).hexdigest()

                    self.__queue[collection].append(data)

                    collection_to_write = self.__readyToWrite()
                    if collection_to_write:
                        self.__write(collection_to_write)
            
            self.__writeAll()

        except Exception as e:
            print(str(e))
    

    def __readyToWrite(self):
        for k in self.__queue.keys():
            if len(self.__queue[k]) == self.__batch_size:
                return k
        return False
        

    def __write(self, collection):
        self.__db.create(collection=collection, data=self.__queue[collection])
        self.__results["total_rows"] += len(self.__queue[collection])
        self.__queue[collection] = []

        
    def __writeAll(self):
        keys = list(self.__queue.keys())
        for k in keys:
            self.__write(k)
    

    def __parseDate(self, date_str):
        # 2021.03.30.01.05.08
        return datetime.datetime.strptime(date_str, "%Y.%m.%d.%H.%M.%S")
