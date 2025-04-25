/* to find the wordcount */

line = LOAD '/pig/file.txt' AS (line:chararray);
words = FOREACH line GENERATE FLATTEN (TOKENIZE(line,' ')) AS word; 
grouped = GROUP words BY word; 
wordcount = FOREACH grouped GENERATE group, COUNT(words); 
Dump wordcount



