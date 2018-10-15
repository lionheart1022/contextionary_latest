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

connectHost().dropDB()

db = Database()

# db.drop()

# create database only once
db.create()

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
    
    
    
    
    
    
    
    
    
    
 