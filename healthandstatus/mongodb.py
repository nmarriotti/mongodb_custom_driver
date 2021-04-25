from pymongo import MongoClient, errors
from bson.objectid import ObjectId
import os
import types
import datetime
import hashlib
import sys
import time
import progressbar
import queue
import threading

class CustomMongodbDriver(object):
    """ CRUD operations """
    
    # Constructor
    def __init__(self, host="127.0.0.1", port=27017, username=None, password=None):
        # Initializing the MongoClient. This helps to 
        # access the MongoDB databases and collections. 
        self.client = MongoClient('{0}:{1}'.format(host, port), username=username, password=password)


    def setDatabase(self, database):
        self.database = self.client[database]


    def create(self, collection=None, data=None):
        ''' Insert document(s) into a collection '''
        if collection is None:
            raise Exception("Collection not specified")
        else:
            if data is not None:
                try:
                    self.database[collection].insert_many(data, ordered=False, bypass_document_validation=True) # data should be dictionary
                    return True # success
                except errors.DuplicateKeyError:
                    pass
                except errors.ConnectionFailure:
                    print("Connection lost")
                    sys.exit(1)
                except:
                    #print(str(e))
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
        self.__overwrite = False
        
        self.dbWriterQueue = queue.Queue()
        
        self.__queue = {}
        self.__results = {
            "files": 0,
            "total_rows": 0,
        }

        self.src = ""
        self.splitchar = ""
        self.sep = ""
        self.traits = []
    
    def setOverwrite(self, value):
        if isinstance(value, bool):
            self.__overwrite = value
        else:
            raise Exception("Overwrite value must be True or False")


    def bulkWriterThread(self, db, name):
        while True:
            try:        
                item = self.dbWriterQueue.get(block=True)
                #print("bulkWriterThread #{0}: {1}".format(name, item["filename"]))
                self.__db.create(collection=item["collection"], data=item["data"])
                self.__db.create(collection="ingestedFiles", data=[{"filename": item["filename"]}])
                self.dbWriterQueue.task_done()
            except:
                pass


    def setIgnoreFirstHeader(self, value):
        if isinstance(value, bool):
            self.__ignore_first_header = value
        else:
            raise Exception("Value must be True or False")

    def __scanDir(self, src, traits):
        q = queue.Queue()

        # Get list of ingested files
        ingestedFiles = [ x["filename"] for x in self.__db.read(collection="ingestedFiles") ]

        # Store files we need to process
        filesToProcess = []

        for subdir, dirs, files in os.walk(src):

            for file in files:

                filename = os.path.join(subdir, file)
                approved = True

                for trait in traits:
                    if not trait in filename:
                        approved = False

                if approved:

                    try:
                        filesToProcess.append(filename)
                    except Exception as e:
                        print(str(e))
        
        if not self.__overwrite:
            try:
                filesToProcess = list(set(filesToProcess) - set(ingestedFiles))
            except Exception as e:
                print(str(e))
                pass

        for item in filesToProcess:
            q.put(item)

        return q
                    

    def start(self, src=None, splitchar="_", sep=",", traits=[]):
        self.src = src
        self.splitchar = splitchar
        self.sep = sep
        self.traits = traits

        self.fileQueue = self.__scanDir(self.src, self.traits)

        self.fileThreads = []
        self.dbThreads = []

        self.fileProgressBar = None
        self.dbWriterProgressBar = None

        if self.fileQueue.qsize() <= 0:
            print("Nothing to do.")
            sys.exit(0)


        # Create progress bar objects
        self.fileProgressBar = progressbar.ProgressBar(maxval=self.fileQueue.qsize(), \
                                    widgets=["Parsing files: ", progressbar.SimpleProgress(), ' ', progressbar.Percentage(), ' ', progressbar.ETA()])

        # Spawn 3 threads to process files
        for i in range(3):
            t = threading.Thread(target=self.processFolder, args=(i,), daemon=True)
            self.fileThreads.append(t)
            t.start()

        # Start 3 bulk writer threads
        for i in range(3):
            t = threading.Thread(target=self.bulkWriterThread, args=(self.__db, i,), daemon=True)
            self.dbThreads.append(t)
            t.start()

        if not self.fileQueue.empty():
            self.fileProgressBar.start()
            while not self.fileQueue.empty():
                self.fileProgressBar.update(self.fileProgressBar.maxval-self.fileQueue.qsize())
                time.sleep(1)
            self.fileProgressBar.finish()



            if not self.dbWriterQueue.empty():
                currentVal = self.dbWriterQueue.qsize()
                self.dbWriterProgressBar = progressbar.ProgressBar(maxval=currentVal, \
                                    widgets=["Writing to database: ", progressbar.SimpleProgress(), ' ', progressbar.Percentage(), ' ', progressbar.ETA()])       
                self.dbWriterProgressBar.start()
                while not self.dbWriterQueue.empty():
                    self.dbWriterProgressBar.update(currentVal-self.dbWriterQueue.qsize())
                    time.sleep(1)
                self.dbWriterProgressBar.finish()

        
        print("\nTotal files:        {0}".format(self.__results["files"]))
        print("Total rows of data: {0}".format(self.__results["total_rows"]))

        print("\nFinished.")      


    def processFolder(self, name):
        while True:
            try:
                filename = self.fileQueue.get(block=True)
                #print("fileProcessThread #{0}: {1}".format(name, filename))
                self.__parseAndAdd(filename, self.splitchar, self.sep)
                self.fileQueue.task_done()
            except Exception as e:
                print(str(e))
                pass


    def __parseAndAdd(self, filename, splitchar, sep):
        queue = {}

        # Tally number of files added to database
        try:
            # Get info from filename
            basename = os.path.basename(filename)
            parts = basename.split(splitchar)
            owner = parts[0]
            collection = parts[1]
            system = parts[2]
            filedate = self.__parseDate(os.path.splitext(parts[3])[0])
            
            # Create unique index of hash column
            self.__db.createIndex(collection=collection, index=[("hash", 'text')])

            queue["collection"] = collection
            queue["filename"] = filename
            queue["data"] = []

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
                    
                    queue["data"].append(data)
                    self.__results["total_rows"] += 1
            
            self.dbWriterQueue.put(queue) 
            self.__results["files"] += 1   

            return True
        except Exception as e:
            print(str(e))

        return False
    

    def __parseDate(self, date_str):
        # 2021.03.30.01.05.08
        return datetime.datetime.strptime(date_str, "%Y.%m.%d.%H.%M.%S")
