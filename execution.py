#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wednesday August 1, 2018

@author: Evelyn

"""
import os
import config
import time
from DatabaseAdapter import connectHost, connectDB

from contextionaryDatabase import Database
import collections
from Context import Context
from Document import Document
import multiprocessing
from multiprocessing import Process
from threading import Thread
from queue import Queue, Empty


class OrderedSet(collections.Set):
    def __init__(self, iterable=()):
        self.d = collections.OrderedDict.fromkeys(iterable)

    def __len__(self):
        return len(self.d)

    def __contains__(self, element):
        return element in self.d

    def __iter__(self):
        return iter(self.d)


start_time = time.time()

connectHost().dropDB()

db = Database()

# db.drop()

# create database only once
db.create()

newContextPaths = []
oldContextPaths = []
newDocumentPaths = []
oldDocumentPaths = []

context = Context()
document = Document()
connectDB = connectDB()


def add_context_process(context_path, connection):
    context.addRecord(context_path, connection)


def delete_context_process(context_path, connection):
    context.deleteRecord(context_path, connection)


def add_document_process(doc_path, connection):
    document.addRecord(doc_path, connection)


def delete_document_process(doc_path, connection):
    document.deleteRecord(doc_path, connection)


root = "Test"

# (1) context table: insert/update/delete
if db.context.Table.exists():
    allContextPaths = []

    for dirpath, dirnames, filenames in os.walk(root):

        if dirnames:

            for contextName in dirnames:
                allContextPaths.append(os.path.join(dirpath, contextName))

    tableContextPaths = db.context.Table.selectColumn("context_path")

    newContextPaths = list(OrderedSet(allContextPaths) - OrderedSet(tableContextPaths))
    oldContextPaths = list(OrderedSet(tableContextPaths) - OrderedSet(allContextPaths))


# (2/3) document table: insert/update/delete
if db.document.Table.exists():

    allDocumentPaths = []

    for dirpath, dirnames, filenames in os.walk(root):

        for documentTitle in filenames:

            if documentTitle.endswith(".txt"):
                allDocumentPaths.append(os.path.join(dirpath, documentTitle))

    tableDocumentPaths = db.document.Table.selectColumn("document_path")

    newDocumentPaths = list(OrderedSet(allDocumentPaths) - OrderedSet(tableDocumentPaths))
    oldDocumentPaths = list(OrderedSet(tableDocumentPaths) - OrderedSet(allDocumentPaths))


class AddDocumentThread(Thread):
    def __init__(self, queue, connection):
        Thread.__init__(self)
        self.queue = queue
        self.connection = connection

    def run(self):
        while True:
            try:
                document_path = self.queue.get_nowait()
                add_document_process(document_path, self.connection)
            except Empty:
                break
        return


class DeleteDocumentThread(Thread):
    def __init__(self, queue, connection):
        Thread.__init__(self)
        self.queue = queue
        self.connection = connection

    def run(self):
        while True:
            try:
                document_path = self.queue.get_nowait()
                delete_document_process(document_path, self.connection)
            except Empty:
                break
        return


if __name__ == '__main__':
    if context.Table.exists():
        if newContextPaths:
            for newContextPath in newContextPaths:
                add_context_process(newContextPath, connectDB)

        if oldContextPaths:
            for oldContextPath in oldContextPaths:
                delete_context_process(oldContextPath, connectDB)

    if document.Table.exists():
        if newDocumentPaths:
            q = Queue()
            for newDocumentPath in newDocumentPaths:
                q.put(newDocumentPath)

            new_threads = []
            for i in range(multiprocessing.cpu_count()):
                t = AddDocumentThread(q, connectDB)
                t.start()
                new_threads.append(t)

            for thread in new_threads:
                thread.join()

        if oldDocumentPaths:
            q = Queue()
            for oldDocumentPath in oldDocumentPaths:
                q.put(oldDocumentPath)

            new_threads = []
            for i in range(multiprocessing.cpu_count()):
                t = DeleteDocumentThread(q, connectDB)
                t.start()
                new_threads.append(t)

            for thread in new_threads:
                thread.join()

# input text unlimited number of times
db.readingComprehensionAssistant("I have some knowledge in mathematics.")
db.readingComprehensionAssistant("I love science, especially mathematics and statistics")

db.updateTables()

end_time = time.time()
print("Done")
print("Execution Time: ", str(end_time-start_time))


### phraseMaxLength
# sets the maximum phrase length for inclusion in phrase_origin and input_text tables

### minPhraseCount
# sets the minimum phrase count for inclusion in phrase_vector_space table (and all other children tables)

### percentileThreshold
# sets percentile threshold for :
    # (1) pairing down phrases in context_phrase table; i.e. discard (1-percentileThreshold)*100 percent
    # (2) pairing down phrases in related_phrase table; i.e. discard (1-percentileThreshold)*100 percent
    # (3) calculating phrase_weight in phrase_weight_by_context table
   
### crossPresenceThreshold
# the minimum cross presence ratio for inclusion in context_phrase
    
### phraseWeightMethod
# select which method ("freq" or "dist") to calculate phrase weight
      
### longPhraseLength
# sets the long phrase length in shared_word table

### similarityThreshold
# sets the maximum Levenshtein distance (similarity_index) for inclusion in phrase_spelling_similarity table

### contextWeightRankThreshold
# sets the maximum rank for inclusion in input_text_context_identifier table based on context_weight
