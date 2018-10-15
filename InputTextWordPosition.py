# -*- coding: utf-8 -*-
"""
Created on Thu Aug 16 12:27:09 2018

@author: estam
"""
from psycopg2 import sql

class InputTextWordPosition(object):
    
    def __init__(self, tableName = "input_text_word_position", copy = False):
        
        from contextionaryDatabase import Table
        
        self.tableName = tableName
        self.copy = copy
        if self.copy == True:
            self.tableName = tableName + '_temp'

        self.defaultColumns = {"input_text_id": "bigint",
                               "input_text_phrase_id": "bigint",
                               "phrase_text": "text",
                               "phrase_position": "bigint",
                               "phrase_length": "bigint",
                               "phrase_components": "bigint[]"
}
        self.defaultPrimaryKeys = ["input_text_id", "input_text_phrase_id"]
        self.defaultForeignKeys = None
        self.defaultUnique = None 
        self.Table = Table(self.tableName)
        self.getTriggerFunction = None
        
    def addRecord(self, inputTextID, inputTextPhraseID, phraseText, phrasePosition, phraseLength, phraseComponents, connectDB):
        
        """
        Adds record to "input text word position" table
        """
  
        cur = connectDB.connection.cursor() 
        try:
            strSQL1 = sql.SQL("""INSERT INTO {} VALUES ({})""")
            strSQL2 = sql.SQL(', ').join(sql.Placeholder() * len(self.defaultColumns))
            cur.execute(strSQL1.format(sql.Identifier(self.tableName), strSQL2), ([inputTextID, inputTextPhraseID, phraseText, phrasePosition, phraseLength, phraseComponents]))
        finally:
            cur.close()
            
            

