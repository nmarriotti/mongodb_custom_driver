import datetime
import os
import sys
import progressbar


def main():
    srcfile = "/tmp/5milrecords.csv"
    file_template = 'ID_ABC-123_Hyperic_system1_DTG.dat'
    destfolder = '/tmp/files'
    items_per_file = 500
    counter = 1
    i = 0

    headers = None
    headers_written = False
    done = False
    filename = ""
    
    with open(srcfile, 'r') as infile:
        

        for line in infile:

            if not headers:
                headers = line
                continue

            if done or filename == "":
                i += 1
                d = datetime.datetime.utcnow().strftime("%Y.%m.%d.%H.%M.%S")
                filename = file_template.replace("DTG", d).replace("ID_", str(i))
                done = False
                headers_written = False

            with open(os.path.join(destfolder, filename), 'a') as outfile:
                if not headers_written:
                    outfile.write(headers)
                    headers_written = True
                else:
                    outfile.write(line)
                    counter += 1

            if counter >= items_per_file:
                done = True
                counter = 0   

            

            



if __name__ == "__main__":
    main()
