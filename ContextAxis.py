# -*- coding: utf-8 -*-
"""
Created on Mon Aug 13 18:47:26 2018

@author: Evelyn Stamey
"""
from contextionaryDatabase import Table
import platform

class ContextAxis(object):
    
    
    def __init__(self, tableName = "context_axis", copy = False):
        
        self.tableName = tableName
        self.copy = copy
        if self.copy == True:
            self.tableName = tableName + '_temp'
        
        self.defaultColumns = {"context_id": "bigint",
                               "independent_context_id": "bigint",
                               "axis_coordinate": "int"}
        self.defaultPrimaryKeys = ["context_id", "independent_context_id"]
        self.defaultForeignKeys = None
        self.tableDependencies = ["context"]
        self.defaultUnique = None
        self.Table = Table(self.tableName)
        self.getTriggerFunction = self.triggerFunction()
        
        
    def triggerFunction(self):
        
        if 'Linux' in platform.platform():
            stringFormatIdentifier = "/"
        else:
            stringFormatIdentifier = "\\\\"

        stringFormat = ("{}".format(stringFormatIdentifier), "{}".format(stringFormatIdentifier))
        
        k = []
        for i in range(len(self.tableDependencies) + 1):
            k.extend(["t" + str(i)])
        v = [self.tableName] + self.tableDependencies
        formatKwargs = dict(zip(k,v))
        
        PL_pgSQL = """CREATE OR REPLACE FUNCTION insertContextAxis()
RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER AS 
$BODY$
    BEGIN
    
        TRUNCATE {t0} CASCADE;
        
        -- t0: context_axis
        -- t1: context

        WITH l_sq4 AS ( -- full outer join r_sq2,l_sq3 to compare directory_split and independent_directory_split
        
            WITH l_sq3 AS ( -- full outer join r_sq1,l_sq2
            
                WITH l_sq2 AS ( -- get all pairwise combinations (context_id, independent_context_id)
                    
                    WITH l_sq1 AS ( -- get all independent_context_id
                    
                        SELECT context_id
                        FROM {t1} 
                        WHERE context_id NOT IN (
                            SELECT DISTINCT parent_id
                            FROM {t1} 
                            WHERE parent_id IS NOT NULL
                        )
                        
                    ) --end l_sq1
                    
                    SELECT DISTINCT
                        {t1}.context_id, 
                        l_sq1.context_id AS independent_context_id

                    FROM {t1}, l_sq1
            
                ) --end l_sq2
            
                SELECT  l_sq2.*,
                    r_sq1.directory_level,
                    r_sq1.directory_split
    
                FROM ( -- split context_path into array of directory names for each context_id
                    
                    SELECT  context_id, 
                            directory_level,
                            regexp_split_to_array(context_path, %s) AS directory_split
                    FROM {t1}
                    
                ) AS "r_sq1" 

                FULL OUTER JOIN l_sq2
                ON r_sq1.context_id = l_sq2.context_id

            ) -- end l_sq3

            SELECT  l_sq3.*,
                    r_sq2.independent_directory_split[1:(directory_level+1)]
                    
            FROM ( -- split context_path into array of directory names for each independent_context_id
            
                    SELECT  regexp_split_to_array(context_path, %s) AS independent_directory_split,
                            context_id AS independent_context_id
                    FROM {t1} 
                    WHERE context_id NOT IN (
                        SELECT DISTINCT parent_id
                        FROM {t1} 
                        WHERE parent_id IS NOT NULL
                    )
                    
            ) AS r_sq2
                    
            FULL OUTER JOIN l_sq3
            ON r_sq2.independent_context_id = l_sq3.independent_context_id
            
        ) -- end l_sq4                 

        INSERT INTO {t0} 
        SELECT  l_sq4.context_id,
                l_sq4.independent_context_id,
                CASE 
                    WHEN l_sq4.directory_split = l_sq4.independent_directory_split THEN 1
                    ELSE 0
                END
        FROM l_sq4
        ON CONFLICT DO NOTHING;

        RETURN NULL;
        
    END;
$BODY$;
CREATE TRIGGER insertContextAxis
AFTER INSERT
ON {t1} 
EXECUTE PROCEDURE insertContextAxis();
"""

        return((PL_pgSQL, formatKwargs, stringFormat))
