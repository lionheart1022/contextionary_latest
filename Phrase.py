# -*- coding: utf-8 -*-
"""
Created on Mon Aug 13 21:27:26 2018

@author: Evelyn Stamey
"""

class Phrase(object):
    

    def __init__(self, tableName = "phrase", copy = False):
        
        from contextionaryDatabase import Table
        
        self.tableName = tableName
        self.copy = copy
        if self.copy == True:
            self.tableName = tableName + '_temp'
        
        self.defaultColumns = {"phrase_id": "serial",
                               "phrase_text": "varchar(255)",
                               "phrase_length": "smallint",
                               "red_flag": "smallint"}
        self.defaultPrimaryKeys = ["phrase_id"]
        self.defaultUnique = ["phrase_text"]
        self.defaultForeignKeys = None
        self.tableDependencies = ["phrase_origin"]
        self.Table = Table(self.tableName)
        self.getTriggerFunction = self.triggerFunction()

        
    def triggerFunction(self):

        
        from nltk.tokenize import RegexpTokenizer
        
        tokenizer = RegexpTokenizer(r'[a-zA-Z0-9_]*[\']{0,1}[a-zA-Z0-9_]+')
        flagWordsString = "the, a, an, I, me, we, us, you, he, him, she, her, it, they, them, my, your, his, her, its, us, our, your, their, myself, himself, herself, itself, ourselves, yourselves, themselves, this, these, that, those, former, latter, who, whom, which, when, where, what, which, whose, why, how, something, anything, nothing, somewhere, anywhere, nowhere, someone, anyone, no one, our, ours, this, some, none, no, thou, thee, ye, aboard, about, above, absent, across, after, against, gainst, again, along, alongst, alongside, amid, amidst, midst, among, amongst, apropos, apud, around, as, astride, at, atop, ontop, before, afore, tofore, behind, ahind, below, ablow, beneath, beside, besides, between, atween, beyond, ayond, but, by, chez, circa, despite, spite, down, during, except, for, from, in, inside, into, less, like, minus, near, nearer, nearest, anear, notwithstanding, of, off, on, onto, opposite, out, outen, outside, over, per, plus, since, than, through,thru, throughout, thruout, till, to, toward, towards, under, underneath, unlike, until, unto, up, upon, upside, versus, via, vis-à-vis, with, within, without, worth, abaft, abeam, aboon, abun, abune, afront, ajax, alongst, aloof, anenst, anent, athwart, atop, ontop, behither, betwixt, atwix, bewest, benorth, emforth, forby, foreanent, forenenst, foregain, foregainst, forth, fromward, froward, fromwards, furth, gainward, imell, inmid, inmiddes, mauger, maugre, nearhand, next, outwith, overthwart, quoad, umbe, umb, unto, uptill, ago, apart, aside, aslant, away, hence, through, withal, for, and, nor, but, or, yet, so, either, both, whether, rather, because, if, while, be, is, isn't,are,ain't, was, wasn't, were, weren't, am, will, shall, won't, should, shouldn't, could, couldn't, would, wouldn't, has, have, had, do, don't, did,didn't"
        flagWords = tokenizer.tokenize(flagWordsString)
        flagWordsRegExp = []
        for flagWord in flagWords:
            if flagWordsRegExp:
                flagWordsRegExp = "|".join([flagWordsRegExp, "^{}[[:space:]]|[[:space:]]{}$|^{}$".format(flagWord, flagWord, flagWord)])
            else:
                flagWordsRegExp = "^{}[[:space:]]|[[:space:]]{}$|^{}$".format(flagWord, flagWord, flagWord)
        stringFormat = [flagWordsRegExp]
        
        k = []
        for i in range(len(self.tableDependencies) + 1):
            k.extend(["t" + str(i)])
        v = [self.tableName] + self.tableDependencies
        formatKwargs = dict(zip(k,v))

        
        PL_pgSQL = """CREATE OR REPLACE FUNCTION insertPhrase()
RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER AS 
$BODY$
    BEGIN
        -- t0: phrase
        -- t1: phrase_origin
        
        TRUNCATE {t0} RESTART IDENTITY CASCADE;
    
        WITH sq1 AS ( -- get array of flag word string matches for each phrase
            SELECT  {t1}.phrase_text, 
                    (LENGTH({t1}.phrase_text) - LENGTH(REPLACE({t1}.phrase_text, ' ', '')) + 1) AS phrase_length, 
                    (SELECT regexp_matches({t1}.phrase_text, %s) AS match_array)
            FROM {t1}
            GROUP BY {t1}.phrase_text
        ) -- end sq1
        
        INSERT INTO {t0} (phrase_text, phrase_length, red_flag)
        SELECT  sq1.phrase_text, 
                sq1.phrase_length, 
                CASE
                    WHEN sq1.match_array IS NULL 
                    THEN 0 
                    ELSE 1 
                END AS red_flag
        FROM sq1
        
        ON CONFLICT DO NOTHING;

        RETURN NULL;
            
    END;
$BODY$;
CREATE TRIGGER insertPhrase
AFTER INSERT
ON {t1}
EXECUTE PROCEDURE insertPhrase();
"""


        return((PL_pgSQL, formatKwargs, stringFormat))