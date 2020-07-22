-- Databricks notebook source
-- MAGIC %python
-- MAGIC 
-- MAGIC from pyspark.sql import functions as f
-- MAGIC from urllib.request import Request, urlopen
-- MAGIC import json
-- MAGIC 
-- MAGIC def send_request(api_object):
-- MAGIC   #API response#
-- MAGIC 
-- MAGIC   jsonResult = []
-- MAGIC   page = 1
-- MAGIC   url = 'https://anapioficeandfire.com/api/'
-- MAGIC   results = True
-- MAGIC   
-- MAGIC   while results and page <= 5: #stopping iteration due to _corrupted_records in api
-- MAGIC     req = Request(url + api_object + '?page=' + str(page) + '&pageSize=25', headers={'User-Agent': 'Mozilla/5.0'})
-- MAGIC     response = urlopen(req).read()
-- MAGIC     results = json.loads(response)  
-- MAGIC     jsonResult.extend(results)
-- MAGIC     page += 1
-- MAGIC     
-- MAGIC   return jsonResult
-- MAGIC   
-- MAGIC def df_char():
-- MAGIC   #Create dataframe for Charaters
-- MAGIC   #
-- MAGIC   dbutils.fs.put("characters.json",  str(send_request('characters')), True) 
-- MAGIC   source_df = spark.read.json("characters.json", multiLine=True)
-- MAGIC   characters_df = source_df['url','name','aliases','gender','titles','books','playedBy']
-- MAGIC   
-- MAGIC   df_r = characters_df.select(characters_df['url'].alias('character_id'),f.explode(characters_df.books).alias('book_id')).withColumn('relationship_type',f.lit('character - book'))
-- MAGIC   df_r.registerTempTable('tmp_char_book_r')
-- MAGIC   characters_df.registerTempTable('tmp_char')
-- MAGIC 
-- MAGIC def df_books():
-- MAGIC   #Create dataframe for Books
-- MAGIC   #
-- MAGIC   dbutils.fs.put("books.json",  str(send_request('books')), True) 
-- MAGIC   source_df = spark.read.json("books.json", multiLine=True )
-- MAGIC   
-- MAGIC   books_df = source_df['url','name','ISBN','Authors','NumberOfPages','Publisher','Country','Released']
-- MAGIC   
-- MAGIC   books_df.registerTempTable('tmp_books')
-- MAGIC   
-- MAGIC def df_houses():
-- MAGIC   #Create dataframe for Houses
-- MAGIC   #
-- MAGIC   dbutils.fs.put("houses.json", str(send_request('houses')), True) 
-- MAGIC   source_df = spark.read.json("houses.json", multiLine=True)
-- MAGIC   houses_df = source_df['url','name','region','overlord','swornMembers']
-- MAGIC   
-- MAGIC   df_r = houses_df.select(houses_df['url'].alias('house_id'),f.explode(houses_df.swornMembers).alias('character_id')).withColumn('relationship_type',f.lit('house - sworn_member'))
-- MAGIC   df_r.registerTempTable('tmp_char_house_r')
-- MAGIC   houses_df.registerTempTable('tmp_houses')
-- MAGIC   
-- MAGIC def main():
-- MAGIC   df_char()
-- MAGIC   df_books()
-- MAGIC   df_houses()
-- MAGIC 
-- MAGIC main()

-- COMMAND ----------

/*Recreate tables*/

CREATE TABLE IF NOT EXISTS characters (character_id int ,name varchar(100), aliases varchar(200), gender varchar(10), titles varchar(300), played_by varchar(200));
CREATE TABLE IF NOT EXISTS books (book_id int,name varchar(100),isbn varchar(20), authors varchar(200), number_of_pages int, publisher varchar(100),country varchar(20),released date);
CREATE TABLE IF NOT EXISTS houses (house_id int, name varchar(100), region varchar(100), overlord_house_id int);
CREATE TABLE IF NOT EXISTS char_book_r (id int, character_id int, book_id int, relationship_type varchar(50));
CREATE TABLE IF NOT EXISTS char_house_r (id int, character_id int, house_id int, relationship_type varchar(50));

/* Populate DB tables */

INSERT OVERWRITE TABLE characters 
SELECT regexp_extract(url,'[^/]+$',0) AS character_id
      ,name                           AS name
      ,array_join(aliases, ', ')      AS aliases
      ,gender                         AS gender
      ,array_join(titles, ', ')       AS titles
      ,array_join(playedBy, ', ')     AS played_by
FROM tmp_char;

INSERT OVERWRITE TABLE books
select regexp_extract(url,'[^/]+$',0) AS book_id
       ,name                          AS name
       ,isbn                          AS isbn
       ,array_join(authors, ', ')     AS authors
       ,numberofpages                 AS number_of_pages
       ,publisher                     AS publisher
       ,country                       AS country
       ,to_date(released)             AS released
FROM tmp_books;

INSERT OVERWRITE TABLE houses
select regexp_extract(url,'[^/]+$',0)      AS house_id 
      ,name                                AS name
      ,region                              AS region
      ,regexp_extract(overlord,'[^/]+$',0) AS overlord_house_id
from tmp_houses;

INSERT OVERWRITE TABLE char_book_r 
select  row_number()over(ORDER BY book_id)      AS id
       ,regexp_extract(character_id,'[^/]+$',0) AS character_id 
       ,regexp_extract(book_id,'[^/]+$',0)      AS book_id 
       ,relationship_type                       AS relationship_type
from tmp_char_book_r;

INSERT OVERWRITE TABLE char_house_r 
select  row_number()OVER(ORDER BY house_id)     AS id
       ,regexp_extract(character_id,'[^/]+$',0) AS character_id 
       ,regexp_extract(house_id,'[^/]+$',0)     AS house_id 
       ,relationship_type                       AS relationship_type
from tmp_char_house_r

-- COMMAND ----------

--2.a Object counts
SELECT COUNT(*), 'character count' FROM characters
UNION ALL
SELECT COUNT(*), 'book count' FROM books
UNION ALL
SELECT COUNT(*), 'house count' FROM houses;


-- COMMAND ----------

--2.b
SELECT c.name AS character_name
      ,c.gender AS character_gender
      ,c.titles AS character_titles
      ,b.name AS book_name
      ,b.authors AS authors
      ,b.released AS release_date 
FROM books b
JOIN char_book_r r ON b.book_id = r.book_id
LEFT JOIN characters c ON c.character_id = r.character_id;

-- COMMAND ----------

--2.b By concatinating character rows to list

SELECT b.name AS book_name
      ,b.authors AS authors
      ,b.released AS release_date 
      ,chr.character as character
FROM books b
LEFT JOIN (SELECT r.book_id, '['||concat_ws('][', collect_list('Name:'||c.name||', Gender:'||c.gender||', Aliases:'||c.aliases))||']' as character FROM characters c
          JOIN char_book_r r ON r.character_id = c.character_id
          GROUP BY r.book_id) chr ON chr.book_id = b.book_id;

-- COMMAND ----------

--2.c
SELECT name AS character_name
      ,aliases AS character_alias
      ,played_by AS actor_name 
FROM characters;

-- COMMAND ----------

--2.d
SELECT h.name AS house_name
       ,h.region AS house_region
       ,ove.name  AS overlords_name
       ,chr.character_names AS character_names
FROM houses h
LEFT JOIN houses ove ON ove.house_id = h.overlord_house_id
LEFT JOIN (SELECT r.house_id, concat_ws(', ', collect_list(c.name)) as character_names FROM characters c
          JOIN char_house_r r ON r.character_id = c.character_id
          WHERE c.name != ''
          GROUP BY r.house_id) chr ON chr.house_id = h.house_id