# -*- coding: utf-8 -*-
"""
Created on Mon Aug 20 13:35:02 2018

@author: Evelyn
"""
import config

class ContextSpellingSimilarity(object):
    
    def __init__(self, tableName = "context_spelling_similarity", copy = False):
        
        from contextionaryDatabase import Table
        
        self.tableName = tableName
        self.copy = copy
        if self.copy == True:
            self.tableName = tableName + '_temp'

        self.defaultColumns = {"context_id": "bigint",
                               "similar_spelling_context_id": "bigint",
                               "similarity_index": "bigint"}
        self.defaultPrimaryKeys = ["context_id", "similar_spelling_context_id"]
        self.defaultForeignKeys = None
        self.tableDependencies = ["context"]
        self.defaultUnique = None
        self.Table = Table(self.tableName)
        self.getTriggerFunction = self.triggerFunction()

   
    def triggerFunction(self):
        
        stringFormat = [config.PARSE['similarityThreshold']]
        
        k = []
        for i in range(len(self.tableDependencies) + 1):
            k.extend(["t" + str(i)])
        v = [self.tableName] + self.tableDependencies
        formatKwargs = dict(zip(k,v))
        
        PL_pgSQL = """CREATE OR REPLACE FUNCTION insertContextSpellingSimilarity()
RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER AS 
$BODY$
    BEGIN
        
        -- t0: context_spelling_similarity
        -- t1: context
        
        TRUNCATE {t0} CASCADE;        

        CREATE EXTENSION IF NOT EXISTS "fuzzystrmatch";

        INSERT INTO {t0} (
        
            SELECT  r_sq1.s_id, 
                    r_sq1.t_id, 
                    levenshtein(r_sq1.s_text, r_sq1.t_text)
            FROM (
            
                SELECT  t1.context_id AS s_id,
        				   t1.context_name AS s_text, 
                        t2.context_id AS t_id, 
                        t2.context_name AS t_text 
                FROM {t1} AS t1
                INNER JOIN {t1} AS t2
                ON t1.context_id < t2.context_id
                
            ) AS r_sq1
            WHERE levenshtein(r_sq1.s_text, r_sq1.t_text) <= %s
        )
        ON CONFLICT DO NOTHING;
        RETURN NULL;
            
    END;
$BODY$;
CREATE TRIGGER insertContextSpellingSimilarity
AFTER INSERT
ON {t1} 
EXECUTE PROCEDURE insertContextSpellingSimilarity();
"""

        return((PL_pgSQL, formatKwargs, stringFormat))

