# -*- coding: utf-8 -*-
"""
Created on Mon Aug 13 18:29:22 2018

@author: Evelyn Stamey
"""
import re
import platform
import config
from Context import Context
from PhraseOrigin import PhraseOrigin
from psycopg2 import connect, sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from collections import Counter
from nltk.tokenize import sent_tokenize, word_tokenize


class Document(object):

    def __init__(self, tableName = "document", copy = True):
        
        from contextionaryDatabase import Table
        from Context import Context

        self.tableName = tableName
        self.copy = copy
        if self.copy == True:
            self.tableName = tableName + '_temp'
            
        context = Context()

        self.defaultColumns = {"document_id": "serial",
                               "document_title": "varchar(255)",
                               "context_id": "bigint",
                               "document_content": "text",
                               "document_path": "text"}
        self.defaultPrimaryKeys = ["document_id"]
        self.defaultUnique = ["document_path"]
        self.defaultForeignKeys = {"context_id": (context.tableName, "context_id")}
        self.getTriggerFunction = None
        self.Table = Table(self.tableName)

    def addRecord(self, documentPath, connectDB):
        
        """
        Adds record to document table and phrase origin table
        """
        print("add document start...", documentPath)
        context = Context()
        phraseOrigin = PhraseOrigin()

        nonPrimeAttributes = [x for x in list(self.defaultColumns.keys()) if x not in self.defaultPrimaryKeys]

        if 'Linux' in platform.platform():
            contextPath = "/".join(documentPath.split("/")[:-1])
            documentFilename = documentPath.split("/")[-1]
        else:
            contextPath = "\\".join(documentPath.split("\\")[:-1])
            documentFilename = documentPath.split("\\")[-1]

        documentTitle = documentFilename[:len(documentFilename)-4]

        contextID = context.Table.selectColumn("context_id", {"context_path": [contextPath]})
        contextID = contextID[0]

        file = open(documentPath, "r", encoding = "UTF-8-sig")
        documentContent = file.read()
        file.close() 
              
        cur = connectDB.connection.cursor()
        try:
            strSQL1 = sql.SQL("""INSERT INTO {} ({}) VALUES ({})""")
            strSQL2 = sql.SQL(', ').join(map(sql.Identifier, nonPrimeAttributes))
            strSQL3 = sql.SQL(', ').join(sql.Placeholder() * len(nonPrimeAttributes))
            cur.execute(strSQL1.format(sql.Identifier(self.tableName), strSQL2, strSQL3), ([documentTitle, contextID, documentContent, documentPath]))
        finally:
            cur.close()

        documentID = self.Table.selectColumn("document_id", {"document_path": [documentPath]})
        
        if phraseOrigin.Table.exists():
            textProcessor = TextProcessor(documentContent, config.PARSE['phraseMaxLength'])
            phraseDictList = textProcessor.phraseCount.values()
            for phraseDict in phraseDictList:
                for key, val in phraseDict.items():
                    phraseOrigin.addRecord(documentID[0], key, val, connectDB)

        print("add document end...", documentPath)

    def deleteRecord(self, documentPath, connectDB):
        
        """
        Deletes record from document table
        """
        cur = connectDB.connection.cursor()
        try:
            strSQL = sql.SQL("""DELETE FROM {} WHERE document_path = %s""")
            cur.execute(strSQL.format(sql.Identifier(self.tableName)), ([documentPath]))
        finally:
            cur.close()


class TextProcessor(object):

    def __init__(self, documentContent, phraseMaxLength):
        
        
        self.documentContent = documentContent
        self.phraseMaxLength = phraseMaxLength
        self.clauses = []
        self.phraseCount = dict()
        self.wordOrderedList = []
        self.size = dict()
        
        
        """
        As a first step once we receive a documentContent, we will break it into clauses using
        the method breakTextIntoClauses.
        """
        self.breakTextIntoClauses()
        
        """
        As a second step, we will break the clauses into phrases of various lengths.
        """
        self.breakClausesIntoPhrases()
   
    
    def getWordOrderedList(self):
        return self.wordOrderedList
    
    def getSize(self):
        return self.size
    
    def getClauses(self):
        return self.clauses
    
    def getPhraseCount(self):
        return self.phraseCount
    
    def breakTextIntoClauses(self):
        
        """
        All commas and semi-columns that usually separate clauses will be replaced
        with dots for convenience
        """
        modifiedText = self.documentContent.replace(",",". .")
        modifiedText = modifiedText.replace(";",". .") 
        modifiedText = modifiedText.replace("(",". .") 
        
        """
        A very useful tool of Python is the sentence tokenizer that splits
        a documentContent into its various sentences and save them into a list of sentences.
        """
        from nltk.tokenize import sent_tokenize
        self.clauses = sent_tokenize(modifiedText)

    
    
    def breakClausesIntoPhrases(self):
        
        """
        A very useful tool of Python is the word tokenizer that splits
        a documentContent into its words and save them into a list of words.
        The list of words will be stored into --wordSplit-- variable.
        """
    
        from nltk.tokenize import word_tokenize
        import re
        
        """
        We define a dictionary {phraselength:phrases} that will store for each
        phraselength the list of phrases that have that length.
        """
        phraseSplit = dict()
        
        for length in range(1, self.phraseMaxLength + 1):
             phraseSplit.update({length:[]})
        
        
        """
        We now look at each of the clauses one by one.
        """
        for clause in self.clauses:
            
            """
            All the words of the clause are stored in a list
            """
            wordSplit=[]
            
            wordSplit=word_tokenize(clause)
            
            wordSplit = [w.lower() for w in wordSplit if re.match("^[A-Za-z0-9_-]*$", w) and not w.startswith('-') and not w.endswith('-') and not w.startswith('_') and not w.endswith('_')]
            
            self.wordOrderedList.extend(wordSplit)
        
            
            """
            All words that start with a capital letter and contain only 1 capital letter will be
            lowered. 
            We transform the documentContent into lower cases to make our analysis non case
            sensitive. Though special words with a capital letter not at the beginning of the word will be left untouched
            and will remain case sensitive.
            """

            for wordindex in range(len(wordSplit)):
                word=wordSplit[wordindex]
                if word[0].isupper() and sum(1 for c in word if c.isupper())==1:        
                    word=word.lower()
                    wordSplit[wordindex]=word

            
            from collections import Counter
            
            """
            We list for each phrase length all the phrases of the clause
            that have that length
            """
            for length in range(1,self.phraseMaxLength+1):
                
                
                for i in range(len(wordSplit)):
                    if i<len(wordSplit)-(length-1):
                        s=wordSplit[i]                        
                        
                        if length>1:
                            for l in range(1,length):
                                s += " " + wordSplit[i+l]
                        
                        # If special symbol in the beginning
                        if (not s[0].isalnum()):
                            i = 0
                            while not s[0].isalnum():
                                i+=1
                                if i == len(s):
                                    break
                            s1 = s[i:]
                        else:
                            s1 = s
                        
                        # If special symbol in the end
                        if (not s[-1].isalnum()):
                            i = 0
                            while not s[-i].isalnum():
                                i+=1
                                if i == len(s):
                                    break
                            s2 = s1[:-i]
                        else:
                            s2 = s1
                        

                        phraseSlice = s2
                        """
                        We add the phrase into the phrases list
                        """
                        phraseSplit[length].append(phraseSlice)
                                                                     
        
        """
        Once all the phrases have been listed, we count their number of occurences
        in the document and we record that into the --phraseCounter-- dictionary.
        We also update the --phraseLengthCounter-- dictionary that tells us for each
        phrase length how many phrases in total exist.
        """
        for length in range(1,self.phraseMaxLength+1):
            self.size.update({length:len(phraseSplit[length])})
            counter=Counter()        
            counter.update(phraseSplit[length]) 
            self.phraseCount.update({length:dict(counter)})
        
