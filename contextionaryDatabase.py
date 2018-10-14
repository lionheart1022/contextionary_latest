# -*- coding: utf-8 -*-
"""
Created on Wednesday August 1, 2018

@author: Evelyn
"""
import config
from psycopg2 import connect, sql 
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from DatabaseAdapter import connectHost, connectDB


class Database(object):    
    def __init__(self):
        self.user = config.DATABASE['user']
        self.password = config.DATABASE['password']
        self.dbname = config.DATABASE['dbname']
        self.rootDirectory = config.PARSE['rootDirectory']
        self.phraseMaxLength = config.PARSE['phraseMaxLength']
        self.minPhraseCount = config.PARSE['minPhraseCount']
        self.percentileThreshold = config.PARSE['percentileThreshold']
        self.crossPresenceThreshold = config.PARSE['crossPresenceThreshold']
        self.phraseWeightMethod = config.PARSE['phraseWeightMethod']
        self.longPhraseLength = config.PARSE['longPhraseLength']
        self.similarityThreshold = config.PARSE['similarityThreshold']
        self.contextWeightRankThreshold = config.PARSE['contextWeightRankThreshold']

        
        from Context import Context
        from ContextAxis import ContextAxis
        from Document import Document
        from PhraseOrigin import PhraseOrigin
        from Phrase import Phrase
        from PhraseMeaning import PhraseMeaning
        from PhraseVectorSpace import PhraseVectorSpace
        from PhraseDistanceToContext import PhraseDistanceToContext
        from ContextPhrase import ContextPhrase
        from RelatedPhrase import RelatedPhrase
        from FrequencyDistance import FrequencyDistance
        if self.phraseWeightMethod == "dist":
            from PhraseWeightByContext import PhraseWeightByContextDistance as PhraseWeightByContext
        elif self.phraseWeightMethod == "freq":
            from PhraseWeightByContext import PhraseWeightByContextFrequency as PhraseWeightByContext
        else:
            print("Invalid input for phraseWeightMethod")
            print("Choose either 'dist' or 'freq'")
        from SharedWord import SharedWord 
        from ContextSpellingSimilarity import ContextSpellingSimilarity        
        from PhraseSpellingSimilarity import PhraseSpellingSimilarity 
        
        from InputText import InputText
        from InputTextPhraseCount import InputTextPhraseCount
        from InputTextWordPosition import InputTextWordPosition
        from InputTextContextIdentifier import InputTextContextIdentifier
        from InputTextKeywords import InputTextKeywords

        # declare variables for all database table classes
        self.context = Context()
        self.document = Document()
        self.phraseOrigin = PhraseOrigin()        
        self.contextAxis = ContextAxis()
        self.contextSpellingSimilarity = ContextSpellingSimilarity()          
        self.phrase = Phrase()
        self.phraseMeaning = PhraseMeaning()
        self.phraseVectorSpace = PhraseVectorSpace()
        self.phraseDistanceToContext = PhraseDistanceToContext()  
        self.contextPhrase = ContextPhrase()
        self.relatedPhrase = RelatedPhrase() 
        self.frequencyDistance = FrequencyDistance() 
        self.phraseWeightByContext = PhraseWeightByContext()  
        self.sharedWord = SharedWord()  
        self.phraseSpellingSimilarity = PhraseSpellingSimilarity()          
        self.inputText = InputText()
        self.inputTextPhraseCount = InputTextPhraseCount()
        self.inputTextWordPosition = InputTextWordPosition()        
        self.inputTextContextIdentifier = InputTextContextIdentifier()
        self.inputTextKeywords = InputTextKeywords()

        # create list of database table classes
        self.databaseTableList = [self.context , 
                                  self.document , 
                                  self.phraseOrigin , 
                                  
                                  self.contextAxis , 
                                  self.contextSpellingSimilarity , 
                                  self.phrase , 
                                  self.phraseMeaning , 
                                  self.phraseVectorSpace ,
                                  self.phraseDistanceToContext ,
                                  self.contextPhrase ,
                                  self.relatedPhrase ,
                                  self.frequencyDistance ,
                                  self.phraseWeightByContext ,
                                  self.sharedWord ,
                                  self.phraseSpellingSimilarity ,
                                  
                                  self.inputText ,
                                  self.inputTextPhraseCount ,
                                  self.inputTextWordPosition ,
                                  
                                  self.inputTextContextIdentifier ,
                                  self.inputTextKeywords
                                  ]
        self.connectHost = connectHost()
        self.connectDB = None
#        # drop database (if database exists)
#        self.drop()
#
#        # create database (if database does not exist)
         # self.create()
#        
#        # udate data in tables
#        self.updateTables()

    def exists(self):
        
        """
        Returns True if database already exists and False, otherwise
        """
        
        cur = self.connectHost.connection.cursor() 
        
        cur.execute("""SELECT EXISTS(SELECT datname FROM pg_catalog.pg_database WHERE datname = %s)""", (self.dbname,))
        fetch_exists = cur.fetchone()
        cur.close() 
        return fetch_exists[0]

    def create(self):
        
        # drop database (if database exists)
        # if self.exists():
            
        #     self.drop()
        
        """
        If database does not exist:
            (1) create database
            (2) create tables
            (3) create trigger functions
            (4) insert data into tables
        """
        # create database (if database does not exist)
        if not self.exists():
                        
            # (1) create database            
            cur = self.connectHost.connection.cursor() 
            try:
                strSQL = sql.SQL("CREATE DATABASE {}")
                cur.execute(strSQL.format(sql.Identifier(self.dbname)))
            finally:
                cur.close() 
                print(" ")
                print("Building \'{}\' database...".format(self.dbname))
                print(" ")
            
            self.connectDB = connectDB()
            # (2) create tables 
            self.createTables(self.databaseTableList)
            
            # (3) create trigger functions
            self.createTriggerFunctions(self.databaseTableList)
            
            # (4) insert data into tables
            # self.updateTables()

    def createTables(self, databaseTableList):
        
        """
        For each database table class in list:
            (1) create table
            (2) add columns
            (3) add primary and foreign key constraints
        """
        for databaseTable in databaseTableList:
            
            # (1) create table
            databaseTable.Table.create()
            
            # (2) add columns
            for key, val in databaseTable.defaultColumns.items():
                databaseTable.Table.addColumn(key, val)
                    
            # (3) add primary and foreign key constraints
            databaseTable.Table.addKeyConstraints(databaseTable.defaultPrimaryKeys, databaseTable.defaultForeignKeys, databaseTable.defaultUnique)
            
            if databaseTable.copy == True:
                templateTable = databaseTable.tableName
                newTable = "_".join(databaseTable.tableName.split("_")[0:-1])
                databaseTable.Table.copy(templateTable, newTable, noData = True)

    def createTriggerFunctions(self, databaseTableList):
        
        for databaseTable in databaseTableList:
            
            if databaseTable.getTriggerFunction:
                
                PL_pgSQL = sql.SQL(databaseTable.getTriggerFunction[0])
                formatKwargs = databaseTable.getTriggerFunction[1]
                stringFormat = databaseTable.getTriggerFunction[2]
                tableIdentifier = {k: sql.Identifier(v) for k,v in formatKwargs.items()}
                
                cur = self.connectDB.connection.cursor() 

                try:
                    cur.execute(PL_pgSQL.format(**tableIdentifier), stringFormat)
                    
#                    wholeFunction = cur.mogrify(PL_pgSQL.format(**tableIdentifier), stringFormat).decode("utf-8-sig") 
#                    if databaseTable.tableName != "phrase_weight_by_context":
#                        f = open(databaseTable.tableName + '_TF.txt','w')
#                        f.write(wholeFunction)
#                        f.close()
#                    else:
#                        f = open(databaseTable.tableName + '_' + self.phraseWeightMethod + '_TF.txt','w')
#                        f.write(wholeFunction)
#                        f.close()

                finally:
                    cur.close()

    def updateTables(self):
        
        """
        Update records in the following tables:
        
            (1) context
            (2) document
            (3) document phrase *
            
        * Note that --document phrase-- is updated indirectly via the Document class
        
        """
        cur = self.connectDB.connection.cursor()
        try:
            strSQL = sql.SQL(''' TRUNCATE context CASCADE;
                                 INSERT INTO context SELECT * FROM context_temp ORDER BY context_id;
                                 TRUNCATE document CASCADE;
                                 INSERT INTO document SELECT * FROM document_temp;
                                 TRUNCATE phrase_origin CASCADE;
                                 INSERT INTO phrase_origin SELECT * FROM phrase_origin_temp;
                                 DROP TABLE context_temp CASCADE;
                                 DROP TABLE document_temp CASCADE;
                                 DROP TABLE phrase_origin_temp CASCADE;
''')
            cur.execute(strSQL)
        finally:
            cur.close()
            
    def readingComprehensionAssistant(self, text):
        
        self.text = text
        self.inputText.addRecord(self.text, self.connectDB)
        

    def drop(self):
        
        """
        If database exists:
            (1) terminate connection
            (2) drop database
        """
        
        if self.exists():
                        
            # (1) terminate connection
            # con = connect(user = self.user, host = "localhost", password = self.password)  
            # con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            cur = self.connectHost.connection.cursor()
            try:    
                strSQL = sql.SQL('''SELECT pid, pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = %s AND pid <> pg_backend_pid()''')
                cur.execute(strSQL, ([self.dbname]))
            finally:
                cur.close()
            
            # (2) drop database
            # con = connect(user = self.user, host = "localhost", password = self.password)  
            # con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            cur = self.connectHost.connection.cursor()
            try:
                strSQL = sql.SQL("DROP DATABASE {}")
                cur.execute(strSQL.format(sql.Identifier(self.dbname)))
            finally:
                # print("Dropped {} database".format(self.dbname))
                cur.close()


                
class Table(object):
    
    def __init__(self, tableName):

        self.tableName = tableName
        self.connectDB = None
        
        
    def checkConnectDB(self):
        if not self.connectDB:
            self.connectDB = connectDB()

    def exists(self):
        
        """
        Returns True if table already exists and False, otherwise
        """
        
        self.checkConnectDB()

        cur = self.connectDB.connection.cursor() 
        try:
            cur.execute('''SELECT EXISTS(SELECT tablename FROM pg_catalog.pg_tables WHERE tablename = %s);''', ([self.tableName]))
            fetch_exists = cur.fetchone()
            return(fetch_exists[0])
        finally:
            cur.close() 
            # con.close() 
            
        
    def create(self):
        
        """
        If table does not exist: create
        """

        self.checkConnectDB()
        
        if not self.exists():
            cur = self.connectDB.connection.cursor()
            try:
                strSQL = sql.SQL("CREATE TABLE {} (_ int)")
                cur.execute(strSQL.format(sql.Identifier(self.tableName)))
            finally:
                cur.close()
                # print(" ")
                # print("Table \'{}\' created".format(self.tableName))
                # print(" ")


    def addColumn(self, columnName, columnDataType):
        
        """
        Adds column --columnName-- with data type --columnDataType-- to database table if column does not exist
        """

        self.checkConnectDB()        
        cur = self.connectDB.connection.cursor()
        try:
            strSQL1 = sql.SQL('''ALTER TABLE {} ADD COLUMN {} ''')
            strSQL2 = sql.SQL(columnDataType)
            cur.execute(sql.Composed([strSQL1.format(sql.Identifier(self.tableName), sql.Identifier(columnName)), strSQL2]))
        finally:
            cur.close()
            # print("Column \'{}\' added to table \'{}\'".format(columnName, self.tableName))
        
        # delete dummy column after first column is added
        self.deleteColumn("_")
        
        
    def selectColumn(self, columnName, whereClause = None):
        
        """
        SELECT columnName FROM tableName WHERE whereClause.keys()[0] IN whereClause.values()[0]
        AND whereClause.keys()[1] IN whereClause.values()[1] AND ...
        """

        self.checkConnectDB()
        cur = self.connectDB.connection.cursor()
        
        try:
            
            strSQL1 = sql.SQL('''SELECT {} FROM {}''')
            sqlCompose = [strSQL1.format(sql.Identifier(columnName), sql.Identifier(self.tableName))]
            executeFormat = None
            
            if whereClause:
                
                WHERE = dict([list(whereClause.items())[0]])
                WHEREkey = list(WHERE.keys())[0]
                WHEREvalue = WHERE[WHEREkey]
                
                AND = dict(list(whereClause.items())[1:])

                strSQL2 = sql.SQL(''' WHERE {} IN ({})''')
                sqlCompose.append(strSQL2.format(sql.Identifier(WHEREkey), sql.SQL(', ').join(sql.Placeholder() * len(WHEREvalue))))
                executeFormat = WHEREvalue
                
                if AND:
                    
                    for key, val in AND.items():

                        strSQL3 = sql.SQL(''' AND {} IN ({})''')
                        sqlCompose.append(strSQL3.format(sql.Identifier(key), sql.SQL(', ').join(sql.Placeholder() * len(val))))
                        executeFormat.extend(val)
                    
            cur.execute(sql.Composed(sqlCompose), executeFormat)
            contextProperty = cur.fetchall()
            contextProperty = [x[0] for x in contextProperty]
            return(contextProperty)
        except IndexError:
            return([])
        finally:
            cur.close() 
        
    def deleteColumn(self, columnName):
        
        """
        Deletes column --columnName-- from database table if column exists
        """

        self.checkConnectDB()
        cur = self.connectDB.connection.cursor()
        try:
            strSQL = sql.SQL('''ALTER TABLE {} DROP IF EXISTS {}''')
            cur.execute(strSQL.format(sql.Identifier(self.tableName), sql.Identifier(columnName)))
        finally:
            cur.close()
            # if columnName != "_":
                # print("Column \'{}\' dropped from table \'{}\'".format(columnName, self.tableName))


    def addKeyConstraints(self, primaryKeys = None, foreignKeys = None, naturalKeys = None):
        
        """
        Adds primary keys --primaryKeys-- and foreign keys --foreignKeys-- to database table
        """

        self.checkConnectDB()
                
        # drop existing primary and foreign key constraints
        self.dropKeyConstraints()

        # add primary keys
        if primaryKeys:
            cur = self.connectDB.connection.cursor()
            try:
                constraint_name = self.tableName + "_pkey"
                strSQL1 = sql.SQL('''ALTER TABLE {} ADD CONSTRAINT {} PRIMARY KEY ({})''')
                strSQL2 = sql.SQL(', ').join(map(sql.Identifier, primaryKeys))
                cur.execute(strSQL1.format(sql.Identifier(self.tableName), sql.Identifier(constraint_name), strSQL2))
            finally:
                cur.close()
        
        # add foreign keys
        if foreignKeys:
            for key, val in foreignKeys.items():
                cur = self.connectDB.connection.cursor()
                try:
                    constraint_name = key + "_fkey"
                    strSQL = sql.SQL('''ALTER TABLE {} ADD CONSTRAINT {} FOREIGN KEY ({}) REFERENCES {}({}) ON DELETE CASCADE ON UPDATE CASCADE''')
                    cur.execute(strSQL.format(sql.Identifier(self.tableName), sql.Identifier(constraint_name), sql.Identifier(key), sql.Identifier(val[0]), sql.Identifier(val[1])))
                finally:
                    cur.close()
                    
        # add unique constraint
        if naturalKeys:
            cur = self.connectDB.connection.cursor()
            try:
                constraint_name = self.tableName + "_unique"
                strSQL1 = sql.SQL('''ALTER TABLE {} ADD CONSTRAINT {} UNIQUE ({})''')
                strSQL2 = sql.SQL(', ').join(map(sql.Identifier, naturalKeys))
                cur.execute(strSQL1.format(sql.Identifier(self.tableName), sql.Identifier(constraint_name), strSQL2))
            finally:
                cur.close()
                    
                    
    def dropKeyConstraints(self):
        
        """
        Drops existing primary and foreign key constraints
        """

        self.checkConnectDB()
        cur = self.connectDB.connection.cursor()
        try:
            strSQL = sql.SQL('''SELECT tc.constraint_name AS foreign_table_name
                               FROM information_schema.table_constraints tc
                               JOIN information_schema.key_column_usage kcu ON tc.constraint_name = kcu.constraint_name
                               JOIN information_schema.constraint_column_usage ccu ON ccu.constraint_name = tc.constraint_name
                               WHERE (constraint_type = 'PRIMARY KEY' OR constraint_type = 'FOREIGN KEY') AND ccu.table_name = %s''')
            cur.execute(strSQL, ([self.tableName]))
            allKeyConstraints = cur.fetchall()
        finally:
            cur.close()

        for keyConstraint in allKeyConstraints:
            cur = self.connectDB.connection.cursor()
            try:
                strSQL = sql.SQL('''ALTER TABLE {} DROP CONSTRAINT IF EXISTS {} CASCADE''')
                cur.execute(strSQL.format(sql.Identifier(self.tableName), sql.Identifier(keyConstraint[0])))
            finally:
                cur.close()
                

    def copy(self, templateTable, newTable, noData = False):
        self.checkConnectDB()
        cur = self.connectDB.connection.cursor()
        try:
            strSQL1 = sql.SQL('''CREATE TABLE {} AS SELECT * FROM {} ''')
            sqlCompose = [strSQL1.format(sql.Identifier(newTable), sql.Identifier(templateTable))]
            if noData == True:
                strSQL2 = sql.SQL(''' WITH NO DATA;''')
                sqlCompose.append(strSQL2)
            cur.execute(sql.Composed(sqlCompose))
        finally:
            cur.close()
                
            
    def drop(self):
        
        """
        Drops table from database
        """
        self.checkConnectDB()
        cur = self.connectDB.connection.cursor()
        
        try:
            strSQL = sql.SQL("""DROP TABLE {}""")
            cur.execute(strSQL.format(sql.Identifier(self.tableName)))
            
        finally:
            cur.close()
            # print("Table \'{}\' dropped".format(self.tableName))


    
