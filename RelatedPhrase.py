# -*- coding: utf-8 -*-
"""
Created on Thu Aug 16 12:27:09 2018

@author: estam
"""
import config

class RelatedPhrase(object):
    
    def __init__(self, tableName = "related_phrase", copy = False):
        
        from contextionaryDatabase import Table
        
        self.tableName = tableName
        self.copy = copy
        if self.copy == True:
            self.tableName = tableName + '_temp'

        self.defaultColumns = {"context_id": "bigint",
                               "context_phrase_id": "bigint",
                               "related_phrase_id":"bigint",
                               "phrase_bonding_index":"decimal"}
        self.defaultPrimaryKeys = ["context_id", "context_phrase_id", "related_phrase_id"]
        self.defaultForeignKeys = None
        self.tableDependencies = ["context_phrase", "phrase", "context_axis", "phrase_origin", "document"]
        self.defaultUnique = None 
        self.Table = Table(self.tableName)
        self.getTriggerFunction = self.triggerFunction()
        #self.getTriggerFunction = None


    def triggerFunction(self):

        stringFormat = [config.PARSE['percentileThreshold']]
        
        k = []
        for i in range(len(self.tableDependencies) + 1):
            k.extend(["t" + str(i)])
        v = [self.tableName] + self.tableDependencies
        formatKwargs = dict(zip(k,v))
        
        PL_pgSQL = """CREATE OR REPLACE FUNCTION insertRelatedPhrase()
RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER AS 
$BODY$
    BEGIN
    
        -- t0: related_phrase
        -- t1: context_phrase
        -- t2: phrase
        -- t3: context_axis
        -- t4: phrase_origin
        -- t5: document
    
        TRUNCATE {t0} CASCADE;
        
        CREATE TABLE "t1t2" AS
            SELECT  r_sq5.context_id, 
                    r_sq5.phrase_id, 
                    array_agg(r_sq5.document_id) -- array of documents within a context that contain the context phrase
            FROM ( --begin r_sq5
            
                SELECT  {t1}.*, 
                        r_sq4.document_id
                FROM ( --begin r_sq4
                
                    SELECT  r_sq3.context_id, 
                            r_sq3.document_id, 
                            r_sq3.phrase_id
                    FROM ( --begin r_sq3
                    
                        SELECT  r_sq2.document_id, 
                                r_sq2.phrase_id, 
                                {t3}.*
                        FROM ( --begin r_sq2
                            
                            SELECT  r_sq1.context_id, 
                                    r_sq1.document_id, 
                                    {t2}.phrase_id
                            FROM ( --begin r_sq1
                            
                                SELECT  {t5}.context_id, 
                                        {t4}.document_id, 
                                        {t4}.phrase_text
                                FROM {t5}
                                INNER JOIN {t4}
                                ON {t5}.document_id = {t4}.document_id
                                
                            ) AS r_sq1 --end r_sq1
                            INNER JOIN {t2}
                            ON r_sq1.phrase_text = {t2}.phrase_text
                            
                        ) AS r_sq2 --end r_sq2
                        FULL OUTER JOIN {t3}
                        ON r_sq2.context_id = {t3}.independent_context_id
                        
                    ) AS r_sq3 --end r_sq3
                    WHERE r_sq3.axis_coordinate = 1
                    ORDER BY r_sq3.context_id, r_sq3.document_id, r_sq3.phrase_id
                
                ) AS r_sq4 --end r_sq3
                
                RIGHT OUTER JOIN {t1}
                ON r_sq4.context_id = {t1}.context_id AND r_sq4.phrase_id = {t1}.phrase_id
                
            ) AS r_sq5 --end r_sq5
            GROUP BY r_sq5.context_id, r_sq5.phrase_id;        

        ---

        CREATE TABLE "all_related_phrase" AS 
        SELECT  l_sq3.context_id, 
                l_sq3.cp_id, 
                l_sq3.rp_id, 
                l_sq3.phrase_bonding_index
        FROM ( --begin l_sq3
        
            WITH l_sq2 AS ( --begin l_sq2
            
                WITH l_sq1 AS ( --begin l_sq1
                
                    SELECT  r_sq1.context_id, 
                            r_sq1.cp_id,
                            r_sq1.cp_docs,
                            array_length(r_sq1.cp_docs, 1) AS n_cp_docs, 
                            r_sq1.rp_id,
                            r_sq1.rp_docs,
                            array_length(r_sq1.rp_docs, 1) AS n_rp_docs, 	
                            ARRAY (
                                SELECT UNNEST(cp_docs)
                                INTERSECT
                                SELECT UNNEST(rp_docs)
                            ) AS shared_docs
                    FROM ( --begin r_sq1
	
                		SELECT  tbl1.context_id, 
                        		tbl1.phrase_id AS cp_id, 
                        		tbl1.array_agg AS cp_docs, 
                        		tbl2.phrase_id AS rp_id,
                        		tbl2.array_agg AS rp_docs
                		FROM t1t2 AS tbl1
                		INNER JOIN t1t2 AS tbl2
                		ON tbl1.context_id = tbl2.context_id AND tbl1.phrase_id > tbl2.phrase_id
        
        		    ) AS r_sq1 --end r_sq1
                    
                ) --end l_sq1
                            
                SELECT  l_sq1.*,
                    	CASE 
                    		WHEN l_sq1.shared_docs = '{{}}' THEN 0
                    		WHEN l_sq1.shared_docs = '{{NULL}}' THEN NULL
                    		ELSE array_length(l_sq1.shared_docs,1)  
                    	END AS n_shared_docs
                FROM l_sq1
                
            ) --end l_sq2
                        
            SELECT  l_sq2.*,
                	CAST (n_shared_docs AS FLOAT)/(n_cp_docs+n_rp_docs-n_shared_docs) AS phrase_bonding_index
            FROM l_sq2
            
        ) AS l_sq3 -- end l_sq3
        ;
                    
        --
        
        INSERT INTO {t0}
        SELECT  context_id, 
                cp_id, 
                rp_id, 
                phrase_bonding_index
        FROM ( --begin r_sq2
        
            SELECT  all_related_phrase.*, 
                    r_sq1.percentile_cont
            FROM all_related_phrase
            FULL OUTER JOIN ( --begin r_sq1
            
                SELECT  context_id, 
                        percentile_cont(%s) WITHIN GROUP (ORDER BY phrase_bonding_index)
                FROM all_related_phrase
                GROUP BY context_id
                
            ) AS r_sq1
            ON all_related_phrase.context_id = r_sq1.context_id
            
        ) AS r_sq2
        WHERE phrase_bonding_index <= percentile_cont
        ON CONFLICT DO NOTHING;
        
        DROP TABLE t1t2;
        DROP TABLE all_related_phrase;
        
        RETURN NULL;
            
    END;
$BODY$;
CREATE TRIGGER insertRelatedPhrase
AFTER INSERT
ON {t1} 
EXECUTE PROCEDURE insertRelatedPhrase();
"""

        return((PL_pgSQL, formatKwargs, stringFormat))

