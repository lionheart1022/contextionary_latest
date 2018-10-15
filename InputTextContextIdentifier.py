# -*- coding: utf-8 -*-
"""
Created on Thu Aug 16 12:27:09 2018

@author: estam
"""
import config

class InputTextContextIdentifier(object):
    
    def __init__(self, tableName = "input_text_context_identifier", copy = False):
        
        from contextionaryDatabase import Table
        
        self.tableName = tableName
        self.copy = copy
        if self.copy == True:
            self.tableName = tableName + '_temp'

        self.defaultColumns = {"input_text_id": "bigint",
                               "context_id": "bigint",
                               "context_weight":"decimal"}
        self.defaultPrimaryKeys = ["input_text_id", "context_id"]
        self.defaultForeignKeys = None
        self.defaultUnique = None 
        self.tableDependencies = ["input_text_phrase_count", "phrase_weight_by_context", "phrase"]
        self.Table = Table(self.tableName)
        self.getTriggerFunction = self.triggerFunction()


    def triggerFunction(self):

        stringFormat = [config.PARSE['contextWeightRankThreshold']]
        
        k = []
        for i in range(len(self.tableDependencies) + 1):
            k.extend(["t" + str(i)])
        v = [self.tableName] + self.tableDependencies
        formatKwargs = dict(zip(k,v))
        
        PL_pgSQL = """CREATE OR REPLACE FUNCTION insertInputTextContextIdentifier()
RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER AS 
$BODY$
    BEGIN

        -- t0: input_text_context_identifier
        -- t1: input_text_phrase_count
        -- t2: phrase_weight_by_context
        -- t3: phrase
    
        TRUNCATE {t0} CASCADE;
        
        INSERT INTO {t0} (
        
            SELECT  r_sq5.input_text_id,
                    r_sq5.context_id,
                    r_sq5.context_weight
            FROM ( --begin r_sq5
            
                SELECT  r_sq4.*,
                        rank() OVER ( PARTITION BY r_sq4.input_text_id ORDER BY r_sq4.context_weight DESC)
                FROM ( --begin r_sq4
                
                    SELECT  r_sq3.input_text_id,
                            r_sq3.context_id,
                            sum(r_sq3.scaled_phrase_weight) AS context_weight
                    FROM ( --begin r_sq3
                    
                        SELECT  r_sq2.input_text_id,
                                r_sq2.context_id,
                                r_sq2.phrase_count*r_sq2.phrase_weight AS scaled_phrase_weight
                        FROM ( --begin r_sq2
                        
                            SELECT  r_sq1.input_text_id,
                                    {t2}.context_id,
                                    r_sq1.phrase_id,
                                    r_sq1.phrase_count,
                                    {t2}.phrase_weight
                            FROM ( --begin r_sq1
                            
                                SELECT  {t1}.input_text_id,
                                        {t3}.phrase_id,
                                        {t1}.phrase_count_per_input AS phrase_count
                                FROM {t3}
                                INNER JOIN {t1}
                                ON {t3}.phrase_text = {t1}.phrase_text
                            
                            ) AS r_sq1
                            INNER JOIN {t2}
                            ON r_sq1.phrase_id = {t2}.phrase_id
                            
                        ) AS r_sq2
                        
                    ) AS r_sq3
                    GROUP BY r_sq3.input_text_id, r_sq3.context_id
                
                ) AS r_sq4
            
            ) AS r_sq5
            WHERE r_sq5.rank <= %s
        
        ) ON CONFLICT DO NOTHING;
            
        RETURN NULL;
            
    END;
$BODY$;
CREATE TRIGGER insertInputTextContextIdentifier
AFTER INSERT
ON {t1} 
EXECUTE PROCEDURE insertInputTextContextIdentifier();
"""

        return((PL_pgSQL, formatKwargs, stringFormat))

