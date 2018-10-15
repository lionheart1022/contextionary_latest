# -*- coding: utf-8 -*-
"""
Created on Mon Aug 20 09:56:41 2018

@author: Evelyn
"""
import config

class SharedWord(object):
    
    def __init__(self, tableName = "shared_word", copy = False):
        
        from contextionaryDatabase import Table
        
        self.tableName = tableName
        self.copy = copy
        if self.copy == True:
            self.tableName = tableName + '_temp'

        self.defaultColumns = {"long_phrase_id": "bigint",
                               "sibling_id": "bigint",
                               "shared_word": "varchar(255)",
                               "shared_word_position_in_long_phrase": "bigint[]",
                               "shared_word_position_in_sibling": "bigint[]"}
        self.defaultPrimaryKeys = ["long_phrase_id", "sibling_id", "shared_word"]
        self.defaultForeignKeys = None
        self.defaultUnique = None 
        self.tableDependencies = ["context_phrase", "phrase"]
        self.Table = Table(self.tableName)
        self.getTriggerFunction = self.triggerFunction()


    def triggerFunction(self):
    
        stringFormat = [config.PARSE['longPhraseLength']]
        
        k = []
        for i in range(len(self.tableDependencies) + 1):
            k.extend(["t" + str(i)])
        v = [self.tableName] + self.tableDependencies
        formatKwargs = dict(zip(k,v))
        
        PL_pgSQL = """CREATE OR REPLACE FUNCTION insertSharedWord()
RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER AS 
$BODY$
    BEGIN
    
        -- t0: shared_word
        -- t1: context_phrase
        -- t2: phrase
    
        TRUNCATE {t0} CASCADE;
        
        CREATE TABLE long_context_phrase AS (
        
            SELECT DISTINCT 
                    {t1}.phrase_id, 
                    r_sq1.phrase_text
            FROM ( --begin r_sq1
            
                SELECT  {t2}.phrase_id,
                        {t2}.phrase_text
                FROM {t2}
                WHERE {t2}.phrase_length >= %s
            
            ) AS r_sq1
            LEFT OUTER JOIN {t1}
            ON r_sq1.phrase_id = {t1}.phrase_id
        
        );


        INSERT INTO {t0} (
        
            SELECT  tbl1.*, 
                    tbl2.shared_word_position_in_sibling
            FROM (
            
                SELECT  r_sq2.long_phrase_id, 
                        r_sq2.sibling_id, 
                        r_sq2.tokenized_long_phrase AS shared_word,
                        array_agg(r_sq2.index) AS shared_word_position_in_long_phrase
                FROM ( --begin r_sq2
                
                    WITH l_sq1 AS ( --begin l_sq1
                    
                        SELECT DISTINCT 
                                r_sq1.phrase_id AS long_phrase_id,
                                r_sq1.sibling_id,
                                to_jsonb(array_to_json(regexp_split_to_array(r_sq1.phrase_text, ' '))) AS long_phrase_text,
                                regexp_split_to_table(r_sq1.phrase_text, ' ') AS tokenized_long_phrase
                        FROM ( --begin r_sq1
                        
                            SELECT  tbl1.*, 
                                    tbl2.phrase_id AS sibling_id, 
                                    tbl2.phrase_text AS sibling_text 
                            FROM long_context_phrase AS tbl1
                            INNER JOIN long_context_phrase AS tbl2
                            ON tbl1.phrase_id < tbl2.phrase_id
                        
                        ) as r_sq1 --end r_sq1
                    
                    ) --end l_sq1
                    
                    SELECT  l_sq1.*, 
                            ordinality AS index
                    FROM l_sq1, jsonb_array_elements_text(l_sq1.long_phrase_text) WITH ordinality
                    WHERE value = l_sq1.tokenized_long_phrase
                
                ) AS r_sq2
                GROUP BY r_sq2.long_phrase_id, r_sq2.sibling_id, r_sq2.tokenized_long_phrase
                
            ) AS tbl1
            INNER JOIN (
            
                SELECT  r_sq2.long_phrase_id, 
                        r_sq2.sibling_id, 
                        r_sq2.sibling_text, 
                        r_sq2.tokenized_long_phrase AS shared_word,
                        array_agg(r_sq2.index) AS shared_word_position_in_sibling
                
                FROM ( --begin r_sq2
                
                    WITH l_sq1 AS ( --begin l_sq1
                    
                        SELECT DISTINCT 
                                r_sq1.phrase_id AS long_phrase_id,
                                r_sq1.sibling_id,
                                to_jsonb(array_to_json(regexp_split_to_array(r_sq1.sibling_text, ' '))) AS sibling_text,
                                regexp_split_to_table(r_sq1.phrase_text, ' ') AS tokenized_long_phrase
                        FROM ( --begin r_sq1
                        
                            SELECT  tbl1.*, 
                                    tbl2.phrase_id AS sibling_id, 
                                    tbl2.phrase_text AS sibling_text 
                            FROM long_context_phrase AS tbl1
                            INNER JOIN long_context_phrase AS tbl2
                            ON tbl1.phrase_id < tbl2.phrase_id
                        
                        ) as r_sq1 --end r_sq1
                    
                    ) --end l_sq1
                    
                    SELECT  l_sq1.*, 
                            ordinality AS index
                    FROM l_sq1, jsonb_array_elements_text(l_sq1.sibling_text) WITH ordinality
                    WHERE value = l_sq1.tokenized_long_phrase
                    
                ) AS r_sq2
                GROUP BY r_sq2.long_phrase_id, r_sq2.sibling_id, r_sq2.sibling_text, r_sq2.tokenized_long_phrase

            ) AS tbl2
            ON tbl1.long_phrase_id = tbl2.long_phrase_id AND tbl1.sibling_id = tbl2.sibling_id AND tbl1.shared_word = tbl2.shared_word
        )
        ON CONFLICT DO NOTHING;
        DROP TABLE long_context_phrase;        
        RETURN NULL;
            
    END;
$BODY$;
CREATE TRIGGER insertSharedWord
AFTER INSERT
ON {t1} 
EXECUTE PROCEDURE insertSharedWord();
"""

        return((PL_pgSQL, formatKwargs, stringFormat))


