# -*- coding: utf-8 -*-
"""
Created on Mon Aug 13 16:11:28 2018

@author: Evelyn Stamey
"""
import platform
import config
from psycopg2 import connect, sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from contextionaryDatabase import Table

class Context(object):
    
    def __init__(self, tableName = "context", copy = True):                
        self.tableName = tableName
        self.copy = copy
        if self.copy == True:
            self.tableName = tableName + '_temp'
            

        
        # dictionary of default columns {key: value}
            # key: column name string
            # value: data type string
        self.defaultColumns = {"context_id": "serial",
                               "context_name": "varchar(255)", 
                               "parent_id": "bigint NULL",
                               "context_children_id": "bigint[] NULL",
                               "context_picture": "varchar(255)",
                               "directory_level": "bigint",
                               "context_path": "text"}
        
        # primary key [item]
            # item: column name string  
        self.defaultPrimaryKeys = ["context_id"]
        
        # list of natural keys (if primary key is a surrogate) [item]
            # item: column name string          
        self.defaultUnique = ["context_path"]

        # dictionary of foreign keys {key: value}
            # key: column name string
            # value: references tuple (first, second)
                # first: reference table name string
                # second: reference column name string  
        self.defaultForeignKeys = {"parent_id": (self.tableName, "context_id")}
        
        # get trigger function string
        self.getTriggerFunction = None
        
        # create table
        self.Table = Table(self.tableName)

    def addRecord(self, contextPath, connectDB):
        
        """
        Adds record to context table
        """
        nonPrimeAttributes = [x for x in list(self.defaultColumns.keys()) if x not in self.defaultPrimaryKeys]

        if 'Linux' in platform.platform():
            dirpath_split = contextPath.split("/")
        else:
            dirpath_split = contextPath.split("\\")
        
        contextName = dirpath_split[-1]
        directoryLevel = len(dirpath_split)-1
        parent = dirpath_split[-2]

        if 'Linux' in platform.platform():
            parentPath = "/".join(dirpath_split[0:-1])
        else:
            parentPath= "\\".join(dirpath_split[0:-1])
        
        contextChildrenID = None
        contextPicture = None
        
        # root = next(os.walk(os.getcwd()))[1][0]
        root = config.PARSE['rootDirectory']
        if parent == root:
            parentID = None
        else:
            cur = connectDB.connection.cursor() 
            try:
                strSQL = sql.SQL("""SELECT context_id FROM {} WHERE context_path = %s""")
                cur.execute(strSQL.format(sql.Identifier(self.tableName)), ([parentPath]))
                parentID = cur.fetchone()
                parentID = parentID[0]
            finally:
                cur.close()
                      
        cur = connectDB.connection.cursor()
        try:
            strSQL1 = sql.SQL("""INSERT INTO {} ({}) VALUES ({})""")
            strSQL2 = sql.SQL(', ').join(map(sql.Identifier, nonPrimeAttributes))
            strSQL3 = sql.SQL(', ').join(sql.Placeholder() * len(nonPrimeAttributes))
            cur.execute(strSQL1.format(sql.Identifier(self.tableName), strSQL2, strSQL3), ([contextName, parentID, contextChildrenID, contextPicture, directoryLevel, contextPath]))
        finally:
            cur.close()

        # update context children ID in --context-- table
        self.generateContextChildrenID(contextPath, connectDB)
            
        # update context picture in --context-- table
        contextPicture = self.generateContextPicture(contextPath)
        self.updateContextProperty(contextPath, {"context_picture": contextPicture}, connectDB)

    def deleteRecord(self, contextPath, connectDB):
        
        """
        Deletes record from context table
        """
        
        cur = connectDB.connection.cursor()
        try:
            strSQL = sql.SQL("""DELETE FROM {} WHERE context_path = %s""")
            cur.execute(strSQL.format(sql.Identifier(self.tableName)), ([contextPath]))
        finally:
            cur.close()
                
 
    def updateContextProperty(self, contextPath, setClause, connectDB): 
        
        """
        Updates record from context table
        """
        
        for key, val in setClause.items():
            
            if key == "parent_id":
                cipid = self.Table.selectColumn("parent_id", {"context_path": [contextPath]})
                key = "context_id"
                contextPath = self.Table.selectColumn("context_path", {key: [cipid[0]]})
            
            cur = connectDB.connection.cursor()
            try:
                strSQL = sql.SQL("""UPDATE {} SET {} = %s WHERE context_path = %s""")
                cur.execute(strSQL.format(sql.Identifier(self.tableName), sql.Identifier(key)), (val, contextPath))
            finally:
                cur.close() 
                # con.close()

    def generateContextChildrenID(self, contextPath, connectDB): 
        
        contextID = self.Table.selectColumn("context_id", {"context_path": [contextPath]})
        parentID = self.Table.selectColumn("parent_id", {"context_path": [contextPath]})

        if parentID[0]:

            contextChildrenID = self.Table.selectColumn("context_children_id", {"context_id": [parentID[0]]})
            
            if contextChildrenID[0] == None:
                contextChildrenID = [contextID[0]]
                parentContextPath = self.Table.selectColumn("context_path", {"context_id": [parentID[0]]})
                self.updateContextProperty(parentContextPath[0], {"context_children_id": contextChildrenID}, connectDB)
            else:
                contextChildrenID[0].extend(contextID)
                parentContextPath = self.Table.selectColumn("context_path", {"context_id": [parentID[0]]})
                self.updateContextProperty(parentContextPath[0], {"context_children_id": contextChildrenID[0]}, connectDB)
            
           
    def generateContextPicture(self, contextPath): 
        
        """
        Generates context picture for context table
        """
        
        contextID = self.Table.selectColumn("context_id", {"context_path": [contextPath]})
        contextName = self.Table.selectColumn("context_name", {"context_path": [contextPath]})
        return("-".join([str(contextID[0]), contextName[0]]))
        
        
            
