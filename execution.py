#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wednesday August 1, 2018

@author: Evelyn

"""
import os
import config
from DatabaseAdapter import connectHost, connectDB

from contextionaryDatabase import Database
import collections
from multiprocessing import Process


class OrderedSet(collections.Set):
    def __init__(self, iterable=()):
        self.d = collections.OrderedDict.fromkeys(iterable)

    def __len__(self):
        return len(self.d)

    def __contains__(self, element):
        return element in self.d

    def __iter__(self):
        return iter(self.d)


connectHost().dropDB()

db = Database()

# db.drop()

# create database only once
db.create()

root = "Test"

# (1) context table: insert/update/delete
if db.context.Table.exists():

    # root = next(os.walk(os.getcwd()))[1][0] # root = "Test"

    allContextPaths = []

    for dirpath, dirnames, filenames in os.walk(root):

        if dirnames:

            for contextName in dirnames:
                allContextPaths.append(os.path.join(dirpath, contextName))

    tableContextPaths = db.context.Table.selectColumn("context_path")

    newContextPaths = list(OrderedSet(allContextPaths) - OrderedSet(tableContextPaths))
    oldContextPaths = list(OrderedSet(tableContextPaths) - OrderedSet(allContextPaths))

    if newContextPaths:
        for newContextPath in newContextPaths:
            db.context.addRecord(newContextPath, db.connectDB)

    if oldContextPaths:
        for oldContextPath in oldContextPaths:
            db.context.deleteRecord(oldContextPath, db.connectDB)

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

    if newDocumentPaths:
        for newDocumentPath in newDocumentPaths:
            db.document.addRecord(newDocumentPath, db.connectDB)

    if oldDocumentPaths:
        for oldDocumentPath in oldDocumentPaths:
            db.document.deleteRecord(oldDocumentPath, db.connectDB)


# input text unlimited number of times
db.readingComprehensionAssistant("I have some knowledge in mathematics.")
db.readingComprehensionAssistant("I love science, especially mathematics and statistics")


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
    
    
    
    
    
    
    
    
    
    
 