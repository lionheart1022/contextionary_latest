# -*- coding: utf-8 -*-
"""
Created on Mon Aug 20 08:51:24 2018

@author: Evelyn
"""
import config

class PhraseWeightByContextDistance(object):
    
    def __init__(self, tableName = "phrase_weight_by_context", copy = False):
        
        from contextionaryDatabase import Table
        
        self.tableName = tableName
        self.copy = copy
        if self.copy == True:
            self.tableName = tableName + '_temp'

        self.defaultColumns = {"context_id": "bigint",
                               "phrase_id": "bigint",
                               "phrase_weight":"decimal"}
        self.defaultPrimaryKeys = ["context_id", "phrase_id"]
        self.defaultForeignKeys = None
        self.tableDependencies = ["context_phrase", "phrase_distance_to_context"]
        self.defaultUnique = None 
        self.Table = Table(self.tableName)
        self.getTriggerFunction = self.triggerFunction()
        


    def triggerFunction(self):
        
        stringFormat = [config.PARSE['percentileThreshold']]
        
        k = []
        for i in range(len(self.tableDependencies) + 1):
            k.extend(["t" + str(i)])
        v = [self.tableName] + self.tableDependencies
        formatKwargs = dict(zip(k,v))
        
        PL_pgSQL = """CREATE OR REPLACE FUNCTION insertPhraseWeightByContext()
RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER AS 
$BODY$
    BEGIN
    
        -- t0: phrase_weight_by_context
        -- t1: context_phrase
        -- t2: phrase_distance_to_context
    
        TRUNCATE {t0} CASCADE;
        
        INSERT INTO {t0}
        SELECT  r_sq4.context_id,
                r_sq4.phrase_id,
                CASE 
                    WHEN (cp_context_id IS NULL AND cp_phrase_id IS NULL) THEN 0
                    ELSE r_sq4.phrase_weight
                END
        FROM ( -- begin r_sq4

            SELECT  r_sq3.*,
                {t1}.context_id AS cp_context_id,
                {t1}.phrase_id AS cp_phrase_id
  
            FROM {t1}
            RIGHT OUTER JOIN ( -- begin r_sq3
            
                SELECT  r_sq2.context_id,
                        r_sq2.phrase_id,
                        CASE 
                            WHEN r_sq2.percentile_cont = 0 THEN 1
                            ELSE (r_sq2.percentile_cont - r_sq2.phrase_distance_to_context)::float/r_sq2.percentile_cont
                        END AS phrase_weight  
                FROM ( -- begin r_sq2
                
                    SELECT  r_sq1.*,
                            {t2}.phrase_id, 
                            {t2}.phrase_distance_to_context
                    FROM ( -- begin r_sq1
                    
                        SELECT  context_id, 
                                percentile_cont(%s) WITHIN GROUP (ORDER BY phrase_distance_to_context) 
                        FROM {t2}
                        GROUP BY context_id
                    
                    ) AS r_sq1
                    FULL OUTER JOIN {t2}
                    ON r_sq1.context_id = {t2}.context_id
                    
                ) as r_sq2
                    
            ) as r_sq3
            ON {t1}.context_id = r_sq3.context_id AND {t1}.phrase_id = r_sq3.phrase_id
        ) as r_sq4
        ON CONFLICT DO NOTHING;

        RETURN NULL;
            
    END;
$BODY$;
CREATE TRIGGER insertPhraseWeightByContext
AFTER INSERT
ON {t1} 
EXECUTE PROCEDURE insertPhraseWeightByContext();
"""

        return((PL_pgSQL, formatKwargs, stringFormat))



class PhraseWeightByContextFrequency(object):
    
    def __init__(self, tableName = "phrase_weight_by_context", copy = False):
        
        from contextionaryDatabase import Table
        
        self.tableName = tableName
        self.copy = copy
        if self.copy == True:
            self.tableName = tableName + '_temp'

        self.defaultColumns = {"context_id": "bigint",
                               "phrase_id": "bigint",
                               "phrase_weight":"decimal"}
        self.defaultPrimaryKeys = ["context_id", "phrase_id"]
        self.defaultForeignKeys = None
        self.tableDependencies = ["context_phrase", "phrase_distance_to_context", "phrase_vector_space"]
        self.defaultUnique = None 
        self.Table = Table(self.tableName)
        self.getTriggerFunction = self.triggerFunction()
        


    def triggerFunction(self):
        
        stringFormat = None
        
        k = []
        for i in range(len(self.tableDependencies) + 1):
            k.extend(["t" + str(i)])
        v = [self.tableName] + self.tableDependencies
        formatKwargs = dict(zip(k,v))
        
        PL_pgSQL = """CREATE OR REPLACE FUNCTION insertPhraseWeightByContext()
RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER AS 
$BODY$
    BEGIN
    
        -- t0: phrase_weight_by_context
        -- t1: context_phrase
        -- t2: phrase_distance_to_context
        -- t2: phrase_vector_space
    
        TRUNCATE {t0} CASCADE;
        
        INSERT INTO {t0} (
        SELECT  r_sq2.context_id,
                r_sq2.phrase_id,
                CASE
                    WHEN r_sq2.cp_context IS NOT NULL
                    THEN 1
                    WHEN r_sq2.cp_context IS NULL AND r_sq2.phrase_relative_frequency IS NOT NULL
                    THEN (r_sq2.phrase_relative_frequency)::float/r_sq2.max
                    ELSE 0
                END AS phrase_weight
        
        FROM ( --begin r_sq2
        
            SELECT r_sq1.*, {t1}.context_id AS cp_context, {t1}.phrase_id AS cp_phrase
            FROM {t1}
            FULL OUTER JOIN ( -- begin r_sq1
            
                SELECT  {t2}.context_id,
                        {t2}.phrase_id,
                        {t3}.phrase_relative_frequency,
                        (SELECT max({t3}.phrase_relative_frequency) FROM {t3})
                FROM {t2}
            		LEFT JOIN {t3}
            		ON {t2}.context_id = {t3}.context_id AND {t2}.phrase_id = {t3}.phrase_id
                                
            ) AS r_sq1
            ON {t1}.context_id = r_sq1.context_id AND {t1}.phrase_id = r_sq1.phrase_id
            
        ) AS r_sq2        
        )

        ON CONFLICT DO NOTHING;

        RETURN NULL;
            
    END;
$BODY$;
CREATE TRIGGER insertPhraseWeightByContext
AFTER INSERT
ON {t1} 
EXECUTE PROCEDURE insertPhraseWeightByContext();
"""

        return((PL_pgSQL, formatKwargs, stringFormat))
