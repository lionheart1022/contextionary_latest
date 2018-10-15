# -*- coding: utf-8 -*-
"""
Created on Thu Aug 16 12:27:09 2018

@author: estam
"""

class InputTextKeywords(object):
    
    def __init__(self, tableName = "input_text_keywords", copy = False):
        
        from contextionaryDatabase import Table
        
        self.tableName = tableName
        self.copy = copy
        if self.copy == True:
            self.tableName = tableName + '_temp'

        self.defaultColumns = {"input_text_id": "bigint",
                               "context_id" : "bigint",
                               "keyword_id": "bigint",
                               "keyword_position": "bigint[]",
                               "keyword_text": "text",
                               "phrase_id": "bigint"}
        self.defaultPrimaryKeys = ["input_text_id", "context_id", "keyword_id"]
        self.defaultForeignKeys = None
        self.defaultUnique = None
        self.tableDependencies = ["input_text_word_position", "input_text_context_identifier", "input_text_phrase_count", "context_phrase", "phrase"]
        self.Table = Table(self.tableName)
        self.getTriggerFunction = self.triggerFunction()


    def triggerFunction(self):

        stringFormat = None
        
        k = []
        for i in range(len(self.tableDependencies) + 1):
            k.extend(["t" + str(i)])
        v = [self.tableName] + self.tableDependencies
        formatKwargs = dict(zip(k,v))
        
        PL_pgSQL = """CREATE OR REPLACE FUNCTION insertInputTextKeywords()
RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER AS 
$BODY$
    BEGIN

        -- t0: input_text_keywords
        -- t1: input_text_word_position
        -- t2: input_text_context_identifier
        -- t3: input_text_phrase_count
        -- t4: context_phrase
        -- t5: phrase
    
        TRUNCATE {t0} CASCADE;

        CREATE TABLE t1t2 AS ( -- finds all keywords, even if keyword is a subset of another keyword
        
            SELECT  {t1}.input_text_id AS itid,
                    r_sq3.cid,
                    {t1}.input_text_phrase_id AS itpid,
                    {t1}.phrase_position AS pp,
                    {t1}.phrase_text AS pt,
                    r_sq3.pid,
                    {t1}.phrase_components AS pc
            FROM (   --select only phrases that are context phrases (within each context) 

                SELECT  r_sq2.input_text_id AS itid,
                        r_sq2.context_id AS cid,
                        r_sq2.phrase_text AS pt,
                        r_sq2.phrase_id AS pid
                FROM ( --select only contexts that are in context identifier
                
                    SELECT  r_sq1.*,
                            {t2}.context_id
                    FROM ( --select only phrases that are in contextionary
                    
                        SELECT  {t3}.input_text_id,
                                {t5}.phrase_text,
                                {t5}.phrase_id
                        FROM {t5}
                        INNER JOIN {t3}
                        ON {t5}.phrase_text = {t3}.phrase_text
                    
                    ) AS r_sq1
                    FULL OUTER JOIN {t2}
                    ON r_sq1.input_text_id = {t2}.input_text_id
                
                ) AS r_sq2
                INNER JOIN {t4}
                ON r_sq2.phrase_id = {t4}.phrase_id AND r_sq2.context_id = {t4}.context_id
                
            
            ) AS r_sq3
            INNER JOIN {t1}
            ON r_sq3.itid = {t1}.input_text_id 
            AND r_sq3.pt = {t1}.phrase_text
        
        );


        INSERT INTO {t0} (
        
            SELECT  t1t2.itid AS input_text_id,
                    t1t2.cid AS context_id,
                    rank() OVER ( PARTITION BY t1t2.itid, t1t2.cid ORDER BY t1t2.pid) AS keyword_id,
                    array_agg(t1t2.pp) AS keyword_position,
                    t1t2.pt AS keyword_text,
                    t1t2.pid AS phrase_id
            FROM t1t2
            WHERE NOT EXISTS  ( -- selects only those keywords in t1t2 that are not subsets of other keywords
            
                SELECT * 
                FROM ( --identifies the keywords within a context and input text that should be dropped from t1t2
                
                    SELECT DISTINCT r_sq1.itid, 
                                    r_sq1.cid,
                                    UNNEST( ARRAY(
                                        SELECT UNNEST(r_sq1.pc1)
                                        INTERSECT
                                        SELECT UNNEST(r_sq1.pc2)
                                    ) ) AS keyword_subset
                    FROM (
                    
                        SELECT tbl1.itid AS itid,
                        tbl1.cid AS cid,
                        tbl1.itpid AS itpid1,
                        tbl1.pp AS pp1,
                        tbl1.pt AS kt1,
                        tbl1.pid AS pid1,
                        tbl1.pc AS pc1,
                        tbl2.itpid AS itpid2,
                        tbl2.pp AS pp2,
                        tbl2.pt AS kt2,
                        tbl2.pid AS pid2,
                        tbl2.pc AS pc2
                        
                        FROM t1t2 AS tbl1
                        INNER JOIN t1t2 AS tbl2
                        ON tbl1.itpid < tbl2.itpid  AND tbl1.itid = tbl2.itid AND tbl1.cid = tbl2.cid
                    
                    ) AS r_sq1
                    
                ) AS r_sq2
                WHERE r_sq2.itid = t1t2.itid AND r_sq2.keyword_subset = t1t2.itpid AND r_sq2.cid = t1t2.cid
            )
            GROUP BY t1t2.itid, t1t2.cid, t1t2.pt, t1t2.pid
        ) 
        ON CONFLICT DO NOTHING;
        DROP TABLE t1t2;
        RETURN NULL;
            
    END;
$BODY$;
CREATE TRIGGER insertInputTextKeywords
AFTER INSERT
ON {t1} 
EXECUTE PROCEDURE insertInputTextKeywords();
"""

        return((PL_pgSQL, formatKwargs, stringFormat))

