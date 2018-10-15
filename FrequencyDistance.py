# -*- coding: utf-8 -*-
"""
Created on Tue Sep  4 15:40:26 2018

@author: Evelyn Stamey
"""

# -*- coding: utf-8 -*-
"""
Created on Thu Aug 16 12:17:58 2018

@author: estam
"""

class FrequencyDistance(object):
    
    def __init__(self, tableName = "frequency_distance", copy = False):
        
        from contextionaryDatabase import Table
        
        self.tableName = tableName
        self.copy = copy
        if self.copy == True:
            self.tableName = tableName + '_temp'

        self.defaultColumns = {"context_id": "bigint",
                               "phrase_id": "bigint",
                               "phrase_relative_frequency": "decimal",
                               "phrase_distance_to_context": "decimal",
                               "phrase_difficulty": "int"}

        self.defaultPrimaryKeys = ["context_id", "phrase_id"]
        self.defaultForeignKeys = None
        self.tableDependencies = ["context_phrase", "phrase_vector_space", "phrase_distance_to_context"]
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
        
        PL_pgSQL = """CREATE OR REPLACE FUNCTION insertFrequencyDistance()
RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER AS 
$BODY$
    BEGIN
    
        -- t0: "frequency_distance"
        -- t1: "context_phrase"
        -- t2: "phrase_vector_space"
        -- t3: "phrase_distance_to_context"
        
        TRUNCATE {t0} CASCADE;
        
        INSERT INTO {t0} (
        
        SELECT  r_sq2.*,
                CASE
                    WHEN (r_sq2.phrase_relative_frequency * 100000) >= 40
                    THEN 1
                    WHEN (r_sq2.phrase_relative_frequency * 100000) >= 20
                    THEN 2
                    WHEN (r_sq2.phrase_relative_frequency * 100000) >= 1
                    THEN 3
                    ELSE 4
                END
        FROM (
        
            SELECT  r_sq1.*,
                    {t3}.phrase_distance_to_context
            FROM ( --begin r_sq1
            
                SELECT  {t1}.context_id,
                        {t1}.phrase_id,
                        {t2}.phrase_relative_frequency
                FROM {t1}
                INNER JOIN {t2}
                ON {t1}.context_id = {t2}.context_id AND {t1}.phrase_id = {t2}.phrase_id
    
            ) AS r_sq1
            INNER JOIN {t3}
            ON r_sq1.context_id = {t3}.context_id AND r_sq1.phrase_id = {t3}.phrase_id
        
        ) AS r_sq2
        
        )
        ON CONFLICT DO NOTHING;
        
        RETURN NULL;
            
    END;
$BODY$;
CREATE TRIGGER insertFrequencyDistance
AFTER INSERT
ON {t1} 
EXECUTE PROCEDURE insertFrequencyDistance();
"""

        return((PL_pgSQL, formatKwargs, stringFormat))


