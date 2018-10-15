# -*- coding: utf-8 -*-
"""
Created on Mon Aug 13 16:11:28 2018

@author: Evelyn Stamey
"""
import config
from psycopg2 import sql

class InputText(object):

    
    def __init__(self, tableName = "input_text", copy = False):
        
        from contextionaryDatabase import Table
        
        self.tableName = tableName
        self.copy = copy
        if self.copy == True:
            self.tableName = tableName + '_temp'

        self.defaultColumns = {"input_text_id": "serial",
                               "input_text": "text"}
        self.defaultPrimaryKeys = ["input_text_id"]
        self.defaultUnique = ["input_text"]
        self.defaultForeignKeys = None
        self.getTriggerFunction = None

        self.Table = Table(self.tableName)


    def addRecord(self, text, connectDB):
        
        """
        Adds record to "input text" and "input text phrase count" and "input text word position"
        This process is analogous to the addRecord module in the Document class
        """
        
        from InputTextPhraseCount import InputTextPhraseCount
        from InputTextWordPosition import InputTextWordPosition
        from psycopg2 import connect, sql 
        from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT 

        nonPrimeAttributes = [x for x in list(self.defaultColumns.keys()) if x not in self.defaultPrimaryKeys]
        
        cur = connectDB.connection.cursor() 
        try:
            strSQL1 = sql.SQL("""INSERT INTO {} ({}) VALUES ({})""")
            strSQL2 = sql.SQL(', ').join(map(sql.Identifier, nonPrimeAttributes))
            strSQL3 = sql.SQL(', ').join(sql.Placeholder() * len(nonPrimeAttributes))
            cur.execute(strSQL1.format(sql.Identifier(self.tableName), strSQL2, strSQL3), ([text]))
        finally:
            cur.close() 
        
        inputTextID = self.Table.selectColumn("input_text_id", {"input_text": [text]})
        inputTextPhraseCount = InputTextPhraseCount()
        
        if inputTextPhraseCount.Table.exists():
            from Document import TextProcessor
            textProcessor = TextProcessor(text, config.PARSE['phraseMaxLength'])
            phraseDictList = textProcessor.phraseCount.values()
            for phraseDict in phraseDictList:
                for key, val in phraseDict.items():
                    inputTextPhraseCount.addRecord(inputTextID[0], key, val, connectDB)
                    

        inputTextWordPosition = InputTextWordPosition()
        if inputTextWordPosition.Table.exists():
            
            maxPL = config.PARSE['phraseMaxLength']
            tp = TextProcessor(text, maxPL)
            PT = tp.getWordOrderedList()
            ID = 0
            PL = 0
            PID_dict = dict()
            for i in range(len(PT), len(PT)-maxPL, -1):
                PL += 1
                PP = list(range(1, len(PT[0:i]) + 1))
                for pp in PP:
                    ID += 1
                    PID_dict.update({(pp,PL) : ID})
                
            PID_dict2 = dict()
            for key, val in PID_dict.items():
                children = []
                PP = key[0]
                PL = key[1]
            
                for pl in range(1, PL+1):
                    start = PID_dict[(PP, pl)]
                    children.extend(list(range(start,start+PL-pl+1)))
                
                ngram = " ".join(PT[(PP-1):(PP+PL-1)])
                dict_key = PID_dict[key]
                dict_value = (key, ngram, children)
                PID_dict2.update({dict_key: dict_value})
                
            for key, val in PID_dict2.items():
                inputTextPhraseID = key
                phraseText = val[1]
                phrasePosition = val[0][0]
                phraseLength = val[0][1]
                phraseComponents = val[2]
                inputTextWordPosition.addRecord(inputTextID[0], inputTextPhraseID, phraseText, phrasePosition, phraseLength, phraseComponents, connectDB)

    
    def deleteRecord(self, text, connectDB):
        
        """
        Deletes record from input text
        """
        
        cur = connectDB.connection.cursor() 
        try:
            strSQL = sql.SQL("""DELETE FROM {} WHERE input_text = %s""")
            cur.execute(strSQL.format(sql.Identifier(self.tableName)), ([text]))
        finally:
            cur.close() 
                
 
#class TextProcessor(object):
#
#    def __init__(self, documentContent, phraseMaxLength):
#        
#        
#        self.documentContent = documentContent
#        self.phraseMaxLength = phraseMaxLength
#        self.clauses = []
#        self.phraseCount = dict()
#        self.wordOrderedList = []
#        self.size = dict()
#        
#        
#        """
#        As a first step once we receive a documentContent, we will break it into clauses using
#        the method breakTextIntoClauses.
#        """
#        self.breakTextIntoClauses()
#        
#        """
#        As a second step, we will break the clauses into phrases of various lengths.
#        """
#        self.breakClausesIntoPhrases()
#   
#    
#    def getWordOrderedList(self):
#        return self.wordOrderedList
#    
#    def getSize(self):
#        return self.size
#    
#    def getClauses(self):
#        return self.clauses
#    
#    def getPhraseCount(self):
#        return self.phraseCount
#    
#    def breakTextIntoClauses(self):
#        
#        """
#        All commas and semi-columns that usually separate clauses will be replaced
#        with dots for convenience
#        """
#        modifiedText = self.documentContent.replace(",",". .")
#        modifiedText = modifiedText.replace(";",". .") 
#        modifiedText = modifiedText.replace("(",". .") 
#        
#        """
#        A very useful tool of Python is the sentence tokenizer that splits
#        a documentContent into its various sentences and save them into a list of sentences.
#        """
#        from nltk.tokenize import sent_tokenize
#        self.clauses = sent_tokenize(modifiedText)
#
#    
#    
#    def breakClausesIntoPhrases(self):
#        
#        """
#        A very useful tool of Python is the word tokenizer that splits
#        a documentContent into its words and save them into a list of words.
#        The list of words will be stored into --wordSplit-- variable.
#        """
#    
#        from nltk.tokenize import word_tokenize
#        import re
#        
#        """
#        We define a dictionary {phraselength:phrases} that will store for each
#        phraselength the list of phrases that have that length.
#        """
#        phraseSplit = dict()
#        
#        for length in range(1, self.phraseMaxLength + 1):
#             phraseSplit.update({length:[]})
#        
#        
#        """
#        We now look at each of the clauses one by one.
#        """
#        for clause in self.clauses:
#            
#            """
#            All the words of the clause are stored in a list
#            """
#            wordSplit=[]
#            
#            wordSplit=word_tokenize(clause)
#            
#            wordSplit = [w.lower() for w in wordSplit if re.match("^[A-Za-z0-9_-]*$", w) and not w.startswith('-') and not w.endswith('-') and not w.startswith('_') and not w.endswith('_')]
#            
#            self.wordOrderedList.extend(wordSplit)
#        
#            
#            """
#            All words that start with a capital letter and contain only 1 capital letter will be
#            lowered. 
#            We transform the documentContent into lower cases to make our analysis non case
#            sensitive. Though special words with a capital letter not at the beginning of the word will be left untouched
#            and will remain case sensitive.
#            """
#
#            for wordindex in range(len(wordSplit)):
#                word=wordSplit[wordindex]
#                if word[0].isupper() and sum(1 for c in word if c.isupper())==1:        
#                    word=word.lower()
#                    wordSplit[wordindex]=word
#
#            
#            from collections import Counter
#            
#            """
#            We list for each phrase length all the phrases of the clause
#            that have that length
#            """
#            for length in range(1,self.phraseMaxLength+1):
#                
#                
#                for i in range(len(wordSplit)):
#                    if i<len(wordSplit)-(length-1):
#                        s=wordSplit[i]                        
#                        
#                        if length>1:
#                            for l in range(1,length):
#                                s += " " + wordSplit[i+l]
#                        
#                        # If special symbol in the beginning
#                        if (not s[0].isalnum()):
#                            i = 0
#                            while not s[0].isalnum():
#                                i+=1
#                                if i == len(s):
#                                    break
#                            s1 = s[i:]
#                        else:
#                            s1 = s
#                        
#                        # If special symbol in the end
#                        if (not s[-1].isalnum()):
#                            i = 0
#                            while not s[-i].isalnum():
#                                i+=1
#                                if i == len(s):
#                                    break
#                            s2 = s1[:-i]
#                        else:
#                            s2 = s1
#                        
#
#                        phraseSlice = s2
#                        """
#                        We add the phrase into the phrases list
#                        """
#                        phraseSplit[length].append(phraseSlice)
#                                                                     
#        
#        """
#        Once all the phrases have been listed, we count their number of occurences
#        in the document and we record that into the --phraseCounter-- dictionary.
#        We also update the --phraseLengthCounter-- dictionary that tells us for each
#        phrase length how many phrases in total exist.
#        """
#        for length in range(1,self.phraseMaxLength+1):
#            self.size.update({length:len(phraseSplit[length])})
#            counter=Counter()        
#            counter.update(phraseSplit[length]) 
#            self.phraseCount.update({length:dict(counter)})
#        
