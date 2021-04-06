from pymongo import MongoClient
from bson.objectid import ObjectId
import os
import types
import datetime


class CustomMongodbDriver(object):
    """ CRUD operations for Animal collection in MongoDB """
    
    # Constructor
    def __init__(self, host="127.0.0.1", port=27017, username=None, password=None):
        # Initializing the MongoClient. This helps to 
        # access the MongoDB databases and collections. 
        self.client = MongoClient('{0}:{1}'.format(host, port),
                                 username=username,
                                 password=password
                                 )

    def setDatabase(self, database):
        self.database = self.client[database]

    def create(self, collection=None, data=None):
        ''' Insert document(s) into a collection '''
        if collection is None:
            raise Exception("Collection not specified")
        else:
            if data is not None:
                try:
                    self.database[collection].insert(data)  # data should be dictionary
                    return True # success
                except Exception as e:
                    print(str(e))
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
    def update(self, collection=None, query=None, changes=None):
        ''' Update data in a collection '''
        if collection is None:
            raise Exception("Collection not specified")
        else:
            if query is not None and changes is not None:
                try:
                    cmd = self.database[collection].update_many(query, { "$set": changes })
                    return cmd.raw_result # json
                except Exception as e:
                    return(str(e))
            else:
                raise Exception("Query and/or update arguments are empty")


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
        self.__batch_size = 1000
        self.__queue = {}
        self.__results = {
            "documents_added": 0,
            "total_rows": 0,
        }
    

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


    def processFolder(self, src=None, splitchar="_", sep=",", traits=[]):
        if not src is None and not splitchar is None:
            try:
                for subdir, dirs, files in os.walk(src):
                    for file in files:
                        filename = os.path.join(subdir, file)

                        approved = True

                        for trait in traits:
                            if not trait in filename:
                                approved = False

                        if approved:
                            self.__parseAndAdd(filename, splitchar, sep)
                            self.__results["documents_added"] += 1

                return self.__results

            except Exception as e:
                print(str(e))

    
    def __parseAndAdd(self, filename, splitchar, sep):
        # Tally number of files added to database
        try:
            # Get info from filename
            parts = os.path.basename(filename).split(splitchar)
            owner = parts[0]
            collection = parts[1]
            system = parts[2]
            filedate = self.__parseDate(os.path.splitext(parts[3])[0])

            if not collection in self.__queue.keys():
                self.__queue[collection] = []

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
        #print(self.__results["total_rows"])
        self.__queue[collection] = []

        
    def __writeAll(self):
        keys = list(self.__queue.keys())
        for k in keys:
            self.__write(k)
    

    def __parseDate(self, date_str):
        # 2021.03.30.01.05.08
        return datetime.datetime.strptime(date_str, "%Y.%m.%d.%H.%M.%S")