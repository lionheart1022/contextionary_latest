# -*- coding: utf-8 -*-
"""
Created on Thu Aug 16 12:17:58 2018

@author: estam
"""
import config

class ContextPhrase(object):
    
    def __init__(self, tableName = "context_phrase", copy = False):
        
        from contextionaryDatabase import Table
        
        self.tableName = tableName
        self.copy = copy
        if self.copy == True:
            self.tableName = tableName + '_temp'

        self.defaultColumns = {"context_id": "bigint",
                               "phrase_id": "bigint"}
        self.defaultPrimaryKeys = ["context_id", "phrase_id"]
        self.defaultForeignKeys = None
        self.tableDependencies = ["phrase_distance_to_context", "context_axis", "phrase_vector_space"]
        self.defaultUnique = None
        self.Table = Table(self.tableName)
        self.getTriggerFunction = self.triggerFunction()

   
    def triggerFunction(self):
        
        stringFormat = [config.PARSE['percentileThreshold'], config.PARSE['crossPresenceThreshold']]
        
        k = []
        for i in range(len(self.tableDependencies) + 1):
            k.extend(["t" + str(i)])
        v = [self.tableName] + self.tableDependencies
        formatKwargs = dict(zip(k,v))
        
        PL_pgSQL = """CREATE OR REPLACE FUNCTION insertContextPhrase()
RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER AS 
$BODY$
    BEGIN
    
        -- t0: context_phrase
        -- t1: phrase_distance_to_context
        -- t2: context_axis
        -- t3: phrase_vector_space
        
        TRUNCATE {t0} CASCADE;
        
        INSERT INTO {t0} (
        SELECT short_phrase_distance.*
        FROM ( --begin short_phrase_distance: context-phrase pairs that have small "phrase_distance_to_context"
        
            SELECT  context_id, phrase_id
            FROM ( --begin r_sq2
            
                SELECT  r_sq1.*,
                        {t1}.phrase_id, 
                        {t1}.phrase_distance_to_context
                FROM (
                
                    SELECT  context_id, 
                            percentile_cont(%s) WITHIN GROUP (ORDER BY phrase_distance_to_context) 
                    FROM {t1}
                    GROUP BY context_id
                
                ) AS r_sq1
                
                FULL OUTER JOIN {t1}
                ON r_sq1.context_id = {t1}.context_id
            
            ) AS r_sq2
                
            WHERE r_sq2.phrase_distance_to_context <= r_sq2.percentile_cont
        
        ) AS short_phrase_distance
        INNER JOIN ( -- begin large_cross_presence: context-phrase pairs that have large cross presence ratios
        
            SELECT  r_sq2.context_id, 
                    r_sq2.phrase_id
            FROM ( --begin r_sq2
            
                SELECT  r_sq1.phrase_id,
                        r_sq1.context_id, 
                        (SELECT CAST (r_sq1.sum AS FLOAT)/r_sq1.n_independent_contexts) AS cross_presence_ratio
                FROM ( --begin r_sq1
                
                    SELECT  tbl1.*,
                            tbl2.n_independent_contexts
                    FROM ( --begin tbl1
                    
                        WITH l_sq1 AS ( --begin l_sq1
                        
                            SELECT  {t2}.*,
                                    {t3}.phrase_id
                            FROM {t2} 
                            LEFT JOIN {t3}
                            ON {t2}.independent_context_id = {t3}.context_id
                            ORDER BY phrase_id, context_id
                        
                        ) --end l_sq1
                        SELECT  l_sq1.phrase_id, 
                                l_sq1.context_id, 
                                sum(l_sq1.axis_coordinate)
                        FROM l_sq1
                        WHERE l_sq1 IS NOT NULL
                        GROUP BY phrase_id, context_id
                        ORDER BY phrase_id, context_id
                        
                    ) AS tbl1
                    LEFT JOIN ( --begin tbl2
                    
                        SELECT  {t2}.context_id, 
                                sum({t2}.axis_coordinate) AS n_independent_contexts
                        FROM {t2}
                        GROUP BY context_id
                    
                    ) AS  tbl2
                    ON tbl1.context_id = tbl2.context_id
                    
                ) AS r_sq1
                
            ) AS r_sq2
            WHERE r_sq2.cross_presence_ratio > %s
        
        ) AS large_cross_presence
        ON short_phrase_distance.phrase_id = large_cross_presence.phrase_id AND short_phrase_distance.context_id = large_cross_presence.context_id
        )

            
        ON CONFLICT DO NOTHING;
        
        RETURN NULL;
            
    END;
$BODY$;
CREATE TRIGGER insertContextPhrase
AFTER INSERT
ON {t1} 
EXECUTE PROCEDURE insertContextPhrase();
"""

        return((PL_pgSQL, formatKwargs, stringFormat))


