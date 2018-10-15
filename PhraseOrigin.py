# -*- coding: utf-8 -*-
"""
Created on Mon Aug 13 18:34:04 2018

@author: Evelyn Stamey
"""
from psycopg2 import sql

class PhraseOrigin(object):
    
    
    def __init__(self, tableName = "phrase_origin", copy = True):
        
        from contextionaryDatabase import Table
        from Document import Document

        self.tableName = tableName
        self.copy = copy
        if self.copy == True:
            self.tableName = tableName + '_temp'
        
        document = Document()
        
        self.defaultColumns = {"document_id": "bigint",
                               "phrase_text": "text",
                               "phrase_count_per_document": "integer"}
        self.defaultPrimaryKeys = ["document_id", "phrase_text"]
        self.defaultUnique = None
        self.defaultForeignKeys = {"document_id": (document.tableName, "document_id")}
        self.Table = Table(self.tableName)
        self.getTriggerFunction = None
        
     
    def addRecord(self, documentID, phraseText, phraseCount, connectDB):
        
        """
        Adds record to phrase origin table
        """        
        cur = connectDB.connection.cursor() 
        try:
            strSQL1 = sql.SQL("""INSERT INTO {} VALUES ({})""")
            strSQL2 = sql.SQL(', ').join(sql.Placeholder() * len(self.defaultColumns))
            cur.execute(strSQL1.format(sql.Identifier(self.tableName), strSQL2), ([documentID, phraseText, phraseCount]))
        finally:
            cur.close()
