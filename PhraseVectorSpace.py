# -*- coding: utf-8 -*-
"""
Created on Tue Aug 14 02:12:01 2018

@author: Evelyn Stamey
"""
import config

class PhraseVectorSpace(object):
    

    def __init__(self, tableName = "phrase_vector_space", copy = False):
        
        from contextionaryDatabase import Table
        
        self.tableName = tableName
        self.copy = copy
        if self.copy == True:
            self.tableName = tableName + '_temp'
        
        self.defaultColumns = {"context_id": "bigint",
                               "phrase_id": "bigint",
                               "phrase_relative_frequency": "decimal"}
        self.defaultPrimaryKeys = ["context_id", "phrase_id"]
        self.defaultUnique = None
        self.defaultForeignKeys = None
        self.tableDependencies = ["phrase_meaning", "phrase"]
        self.Table = Table(self.tableName) 
        self.getTriggerFunction = self.triggerFunction()
        
    def triggerFunction(self):
        
        stringFormat = [config.PARSE['minPhraseCount']]
        
        k = []
        for i in range(len(self.tableDependencies) + 1):
            k.extend(["t" + str(i)])
        v = [self.tableName] + self.tableDependencies
        formatKwargs = dict(zip(k,v))
        
        PL_pgSQL = """CREATE OR REPLACE FUNCTION insertPhraseVectorSpace()
RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER AS 
$BODY$
    BEGIN

        -- t0: phrase_vector_space
        -- t1: phrase_meaning
        -- t2: phrase

        TRUNCATE {t0} CASCADE;

        CREATE TABLE "frequent_phrase" AS

        SELECT  r_sq2.context_id,
                {t2}.phrase_id AS frequent_phrase_id,
                {t2}.phrase_length,
                r_sq2.phrase_count_per_context
        FROM {t2}
        INNER JOIN ( -- begin r_sq2
        
            SELECT {t1}.* 
            FROM {t1}
            INNER JOIN ( -- begin r_sq1
            
                SELECT DISTINCT phrase_id AS frequent_phrase_id
                FROM {t1}
                WHERE phrase_count_per_context >= %s
            
            ) AS r_sq1
            ON {t1}.phrase_id = r_sq1.frequent_phrase_id
        
        ) AS r_sq2
        ON r_sq2.phrase_id = {t2}.phrase_id;


        WITH l_sq2 AS ( --begin l_sq2

            WITH l_sq1 AS ( --begin l_sq1
        
                SELECT  frequent_phrase.*,
                        r_sq1.frequent_ngram_count
                FROM frequent_phrase
                INNER JOIN ( -- for each pair (independent_context_id, phrase_length) get frequent ngram count per context
                        
                    SELECT  frequent_phrase.context_id,
                            frequent_phrase.phrase_length,
                            sum(frequent_phrase.phrase_count_per_context) AS frequent_ngram_count
                    FROM frequent_phrase
                    GROUP BY frequent_phrase.context_id, frequent_phrase.phrase_length
                        
                ) AS r_sq1   
                ON frequent_phrase.context_id = r_sq1.context_id
                AND frequent_phrase.phrase_length = r_sq1.phrase_length
        
            ) -- end l_sq1
    
            SELECT  l_sq1.context_id,
                    l_sq1.frequent_phrase_id AS phrase_id,
                    (SELECT CAST (l_sq1.phrase_count_per_context AS FLOAT)/l_sq1.frequent_ngram_count) AS phrase_relative_frequency
            FROM l_sq1
            
        ) --end l_sq2
                
        INSERT INTO {t0}
        SELECT * FROM l_sq2
        ON CONFLICT (context_id, phrase_id) DO NOTHING;
        
        DROP TABLE "frequent_phrase";

        RETURN NULL;
        
        
    END;
$BODY$;
CREATE TRIGGER insertPhraseVectorSpace
AFTER INSERT
ON {t1} 
EXECUTE PROCEDURE insertPhraseVectorSpace();

"""

        return((PL_pgSQL, formatKwargs, stringFormat))
