# -*- coding: utf-8 -*-
"""
Created on Mon Aug 20 13:35:02 2018

@author: Evelyn
"""
import config

class PhraseSpellingSimilarity(object):
    
    def __init__(self, tableName = "phrase_spelling_similarity", copy = False):
        
        from contextionaryDatabase import Table
        
        self.tableName = tableName
        self.copy = copy
        if self.copy == True:
            self.tableName = tableName + '_temp'

        self.defaultColumns = {"phrase_id": "bigint",
                               "similar_spelling_phrase_id": "bigint",
                               "similarity_index": "bigint"}
        self.defaultPrimaryKeys = ["phrase_id", "similar_spelling_phrase_id"]
        self.defaultForeignKeys = None
        self.defaultUnique = None
        self.tableDependencies = ["context_phrase", "phrase"]
        self.Table = Table(self.tableName)
        self.getTriggerFunction = self.triggerFunction()

   
    def triggerFunction(self):
        
        stringFormat = [config.PARSE['similarityThreshold']]
        
        k = []
        for i in range(len(self.tableDependencies) + 1):
            k.extend(["t" + str(i)])
        v = [self.tableName] + self.tableDependencies
        formatKwargs = dict(zip(k,v))
        
        PL_pgSQL = """CREATE OR REPLACE FUNCTION insertPhraseSpellingSimilarity()
RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER AS 
$BODY$
    BEGIN
    
        -- t0: phrase_spelling_similarity
        -- t1: context_phrase
        -- t2: phrase
    
        TRUNCATE {t0} CASCADE;    
        
        CREATE TABLE context_phrase_text AS (
        
            SELECT DISTINCT 
                    {t1}.phrase_id, 
                    r_sq1.phrase_text
            FROM ( --begin r_sq1
            
                SELECT  {t2}.phrase_id,
                        {t2}.phrase_text
                FROM {t2}
            
            ) AS r_sq1
            LEFT OUTER JOIN {t1}
            ON r_sq1.phrase_id = {t1}.phrase_id
        
        );

        CREATE EXTENSION IF NOT EXISTS "fuzzystrmatch";

        INSERT INTO {t0} (
        
            SELECT  r_sq1.s_id, 
                    r_sq1.t_id, 
                    levenshtein(r_sq1.s_text, r_sq1.t_text)
            FROM (
            
                SELECT  tbl1.phrase_id AS s_id,
                        tbl1.phrase_text AS s_text, 
                        tbl2.phrase_id AS t_id, 
                        tbl2.phrase_text AS t_text 
                FROM context_phrase_text AS tbl1
                INNER JOIN context_phrase_text AS tbl2
                ON tbl1.phrase_id < tbl2.phrase_id
                
            ) AS r_sq1
            WHERE levenshtein(r_sq1.s_text, r_sq1.t_text) <= %s
        )
        ON CONFLICT DO NOTHING;
       
        DROP TABLE context_phrase_text;

        RETURN NULL;
            
    END;
$BODY$;
CREATE TRIGGER insertPhraseSpellingSimilarity
AFTER INSERT
ON {t1} 
EXECUTE PROCEDURE insertPhraseSpellingSimilarity();
"""

        return((PL_pgSQL, formatKwargs, stringFormat))

