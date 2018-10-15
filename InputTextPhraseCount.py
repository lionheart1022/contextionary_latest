# -*- coding: utf-8 -*-
"""
Created on Mon Aug 13 18:34:04 2018

@author: Evelyn Stamey
"""
from psycopg2 import sql

class InputTextPhraseCount(object):
    
    
    def __init__(self, tableName = "input_text_phrase_count", copy = False):
        
        from contextionaryDatabase import Table
        
        self.tableName = tableName
        self.copy = copy
        if self.copy == True:
            self.tableName = tableName + '_temp'
        
        self.defaultColumns = {"input_text_id": "bigint",
                               "phrase_text": "text",
                               "phrase_count_per_input": "integer"}
        self.defaultPrimaryKeys = ["input_text_id", "phrase_text"]
        self.defaultUnique = None
        self.defaultForeignKeys = None
        self.Table = Table(self.tableName)
        self.getTriggerFunction = None
        
     
    def addRecord(self, inputTextID, phraseText, phraseCount, connectDB):
        
        """
        Adds record to phrase origin table
        """
  
        cur = connectDB.connection.cursor() 
        try:
            strSQL1 = sql.SQL("""INSERT INTO {} VALUES ({})""")
            strSQL2 = sql.SQL(', ').join(sql.Placeholder() * len(self.defaultColumns))
            cur.execute(strSQL1.format(sql.Identifier(self.tableName), strSQL2), ([inputTextID, phraseText, phraseCount]))
        finally:
            cur.close()
