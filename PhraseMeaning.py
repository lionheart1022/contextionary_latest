# -*- coding: utf-8 -*-
"""
Created on Tue Aug 14 02:09:24 2018

@author: Evelyn Stamey
"""

class PhraseMeaning(object):
    
    
    def __init__(self, tableName = "phrase_meaning", copy = False):
        
        from contextionaryDatabase import Table
        
        self.tableName = tableName
        self.copy = copy
        if self.copy == True:
            self.tableName = tableName + '_temp'
        
        self.defaultColumns = {"context_id": "bigint",
                               "phrase_id": "bigint",
                               "phrase_count_per_context": "integer"
                               }

        self.defaultPrimaryKeys = ["context_id", "phrase_id"]
        self.defaultUnique = None
        self.defaultForeignKeys = None
        self.tableDependencies = ["phrase", "phrase_origin", "document", "context_axis"]
        self.Table = Table(self.tableName)  
        self.getTriggerFunction = self.triggerFunction()
        
    def triggerFunction(self):
        
        stringFormat = None
        
        k = []
        for i in range(len(self.tableDependencies) + 1):
            k.extend(["t" + str(i)])
        v = [self.tableName] + self.tableDependencies
        formatKwargs = dict(zip(k,v))
        
        PL_pgSQL = """CREATE OR REPLACE FUNCTION insertPhraseMeaning()
RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER AS 
$BODY$
    BEGIN
    
        -- t0: phrase_meaning
        -- t1: phrase
        -- t2: phrase_origin
        -- t3: document
        -- t4: context_axis
        
        TRUNCATE {t0} CASCADE;
        
        WITH l_sq2 AS ( --begin l_sq2
    
            WITH l_sq1 AS ( --begin l_sq1
                    
                SELECT  {t3}.document_id, 
                        {t3}.context_id AS independent_context_id, 
                        {t2}.phrase_text, 
                        {t2}.phrase_count_per_document 
                FROM {t3} 
                INNER JOIN {t2}
                ON {t2}.document_id = {t3}.document_id
            ) --end l_sq1
        
            SELECT  l_sq1.independent_context_id, 
            
                    (
                    SELECT {t1}.phrase_id 
                    FROM {t1} 
                    WHERE {t1}.phrase_text = l_sq1.phrase_text
                    AND {t1}.red_flag = 0
                    ) AS phrase_id,
                    
                    sum(l_sq1.phrase_count_per_document) AS phrase_count_per_context
            FROM l_sq1
            GROUP BY l_sq1.independent_context_id, l_sq1.phrase_text
                
        ) --end l_sq2
        
        INSERT INTO {t0} -- first, insert data for independent contexts
        SELECT  l_sq2.independent_context_id,
                l_sq2.phrase_id,
                l_sq2.phrase_count_per_context
        FROM l_sq2
        WHERE phrase_id IS NOT NULL
        
        ON CONFLICT (context_id, phrase_id) DO NOTHING;
        
        
        WITH l_sq1 AS ( --begin l_sq1
        
            SELECT  r_sq1.context_id,
                    {t0}.phrase_id,
                    {t0}.phrase_count_per_context AS phrase_count_per_ic
            FROM {t0}
            FULL OUTER JOIN ( --begin r_sq1
            
                SELECT * 
                FROM {t4}
                WHERE axis_coordinate = 1
                AND context_id <> independent_context_id
                
            ) as r_sq1
            ON {t0}.context_id = r_sq1.independent_context_id
        )
        
        INSERT INTO {t0} -- then insert data for dependent contexts
        SELECT  l_sq1.context_id, 
                l_sq1.phrase_id,
                sum(l_sq1.phrase_count_per_ic) AS phrase_count_per_context
        FROM l_sq1
        GROUP BY l_sq1.context_id, l_sq1.phrase_id
        
        ON CONFLICT (context_id, phrase_id) DO NOTHING;
 
        RETURN NULL;
        
    END;
$BODY$;
CREATE TRIGGER insertPhraseMeaning
AFTER INSERT
ON {t1}
EXECUTE PROCEDURE insertPhraseMeaning();
"""
        return((PL_pgSQL, formatKwargs, stringFormat))


