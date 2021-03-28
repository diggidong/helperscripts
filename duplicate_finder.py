#!/usr/bin/env python

import argparse
import datetime
import hashlib
from pathlib import Path
from threading import Thread, Semaphore
from time import perf_counter
import os

isFile = []
isDir = []
isSpecialFile = []
foundFiles = {}
filesWithDuplicates = {}

def createRecursiveList(path):
    # source: https://stackoverflow.com/questions/18394147/recursive-sub-folder-search-and-return-files-in-a-list-python
    print(f'Creating a recursive list on path: {path}')
    result = list(Path(path).rglob("*"))
    
    return result

def createHash(file):
    # source: https://nitratine.net/blog/post/how-to-hash-files-in-python/
    sema.acquire()
    BLOCK_SIZE = 65536
    
    file_hash = hashlib.sha256()
    if processFileType(file):
        with open(str(file), 'rb') as f:
            fb = f.read(BLOCK_SIZE)
            while len(fb) > 0:
                file_hash.update(fb)
                fb = f.read(BLOCK_SIZE)
    
        hash = file_hash.hexdigest()
        addFoundFilesToList(hash, file)
    # source: https://stackoverflow.com/questions/16874598/how-do-i-calculate-the-md5-checksum-of-a-file-in-python
    # but needs more RAM
    #if processFileType(file):
    #    hash = hashlib.sha256(open(str(file), 'rb').read()).hexdigest()
    #    addFoundFilesToList(hash, file)
    sema.release()

def processFileType(file):
    if os.path.isdir(file):
        isDir.append(file)
        return False
    elif os.path.isfile(file):
        isFile.append(file)
        return True
    else:
        isSpecialFile.append(file)
        return False

def addFoundFilesToList(hash, file):
    global foundFiles
    if hash in foundFiles.keys():
        foundFiles[hash].append(file)
    else:
        foundFiles[hash] = [file]

def findDuplicates():
    global foundFiles
    for entry in foundFiles:
        if len(foundFiles[entry]) > 1:
            filesWithDuplicates[entry] = foundFiles[entry]

def writeToFile(path, outputfile, duration):
    f = open(outputfile, 'a')
    f.write("*" * 80 + "\n")
    f.write("Search for duplicates on path: '" + path + "' - Start at: " + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + f" - Duration: {duration:0.2f}\n\n")

    for key in filesWithDuplicates:
        f.write("sha256-Hash: " + key + "\n")
        f.write("Duplicates:" + "\n")
        for value in filesWithDuplicates[key]:
            f.write("'" + str(value) + "'\n")
        f.write("\n")
    
    f.write("\n")
    f.close()
    
# TODO
def deleteAttempt():
    choice = input("Do you want to delete the duplicate files in an automated manner? (y/n)")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description = 'Find and delete duplicates on a given path recursively.')
    parser.add_argument('-t', dest = 'threads', type = int, default = 8, help = 'Define the max threads to be used (default is 8).')
    parser.add_argument('-p', dest = 'path', type = str, default = '.', help = 'Define the path to check recursively for duplicates.')
    parser.add_argument('-o', dest = 'outputfile', type = str, default = 'duplicates.txt', help = 'Print duplicates to a file.')

    args = parser.parse_args()
    
    sema = Semaphore(value = args.threads)
    threads = []

    start_time = perf_counter()
    files = createRecursiveList(args.path)

    for file in files:
        thread = Thread(target = createHash, args = (file,))
        threads.append(thread)
        thread.start()
    
    for thread in threads:
        thread.join()

    findDuplicates()
    end_time = perf_counter()
    duration = end_time - start_time
    writeToFile(args.path, args.outputfile, duration)

    #output
    #print("Found duplicates:")
    #print(filesWithDuplicates)
