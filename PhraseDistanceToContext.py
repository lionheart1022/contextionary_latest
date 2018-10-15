# -*- coding: utf-8 -*-
"""
Created on Thu Aug 16 12:00:32 2018

@author: estam
"""

class PhraseDistanceToContext(object):
    
    def __init__(self, tableName = "phrase_distance_to_context", copy = False):
        
        from contextionaryDatabase import Table

        self.tableName = tableName
        self.copy = copy
        if self.copy == True:
            self.tableName = tableName + '_temp'
        
        self.defaultColumns = {"context_id": "bigint",
                               "phrase_id": "bigint",
                               "phrase_distance_to_context": "decimal"}
        self.defaultPrimaryKeys = ["context_id", "phrase_id"]
        self.defaultForeignKeys = None
        self.tableDependencies = ["phrase_vector_space", "context_axis"]
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
        
        PL_pgSQL = """CREATE OR REPLACE FUNCTION insertPhraseDistanceToContext()
RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER AS 
$BODY$
    BEGIN
        -- t0: phrase_distance_to_context
        -- t1: phrase_vector_space
        -- t2: context_axis
    
        TRUNCATE {t0} CASCADE;

    
        INSERT INTO {t0}  
        SELECT  r_sq8.* 
        FROM (
        
            SELECT  r_sq7.context_id, 
                    r_sq7.phrase_id,
                    SUM(r_sq7.diff_prp_pp * r_sq7.diff_prp_pp)^0.5 AS phrase_distance
            FROM (
                    
                SELECT  r_sq6.context_id, 
                        r_sq6.phrase_id,
                        r_sq6.phrase_relative_frequency-r_sq6."phraseProjection" AS diff_prp_pp
                FROM (   
                
                    SELECT  r_sq5.context_id, 
                            r_sq5.phrase_id,
                            {t1}.context_id AS ICID, 
                            CASE
                                WHEN {t1}.phrase_relative_frequency IS NULL
                                THEN 0
                                ELSE {t1}.phrase_relative_frequency
                            END,
                            r_sq5."phraseProjection" 
                    FROM {t1}
                    RIGHT JOIN (
                    
                        SELECT  r_sq4.context_id, 
                                {t1}.phrase_id, 
                                r_sq4.ICIDi, 
                                SUM({t1}.phrase_relative_frequency * r_sq4."P2") AS "phraseProjection"
                        FROM {t1}, (
                
                            SELECT DISTINCT 
                                {t1}.phrase_id,
                                r_sq3.context_id,
                                r_sq3.ICIDi,
                                r_sq3.ICIDj,
                                r_sq3."P2"
                            FROM {t1}, (
                                    
                                SELECT  r_sq2.context_id, 
                                        {t2}.independent_context_id AS ICIDi, 
                                        r_sq2.independent_context_id AS ICIDj, 
                                        SUM({t2}.axis_coordinate * r_sq2."P1") AS "P2"
                                FROM {t2}, (
                                
                                    SELECT  {t2}.independent_context_id, 
                                            {t2}.context_id, 
                                            {t2}.axis_coordinate * r_sq1."ITRR" AS "P1"
                                    FROM {t2}, (
                                    
                                        SELECT  {t2}.context_id, 
                                                CAST (1 AS FLOAT)/SUM({t2}.axis_coordinate * {t2}.axis_coordinate) AS "ITRR" 
                                        FROM {t2} 
                                        GROUP BY {t2}.context_id
                                        
                                    ) AS r_sq1
                                    
                                    WHERE {t2}.context_id = r_sq1.context_id  
                                    
                                ) AS r_sq2
                                    
                                WHERE {t2}.context_id = r_sq2.context_id
                                GROUP BY {t2}.independent_context_id, r_sq2.independent_context_id, r_sq2.context_id
                                    
                            ) AS r_sq3
                                    
                        ) AS r_sq4
            
                        WHERE {t1}.context_id = r_sq4.ICIDj AND {t1}.phrase_id = r_sq4.phrase_id
                        GROUP BY {t1}.phrase_id, r_sq4.ICIDi, r_sq4.context_id
                        
                    ) AS r_sq5
                                
                    ON {t1}.context_id = r_sq5.ICIDi AND {t1}.phrase_id = r_sq5.phrase_id
                    
                ) AS r_sq6
                
            ) AS r_sq7
                            
            GROUP BY r_sq7.context_id, r_sq7.phrase_id
            
        ) AS r_sq8 ON CONFLICT DO NOTHING;

        RETURN NULL;
            
    END;
$BODY$;
CREATE TRIGGER insertPhraseDistanceToContext
AFTER INSERT
ON {t1} 
EXECUTE PROCEDURE insertPhraseDistanceToContext();
"""

        return((PL_pgSQL, formatKwargs, stringFormat))