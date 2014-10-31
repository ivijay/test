 /* rmf /tmp/pig/weblog; */
input1 = LOAD '/home/HadoopUser/Desktop/ques/pig-poc/input.log' AS (line:chararray);
split_input = foreach input1 GENERATE FLATTEN(STRSPLIT(line,'"'));
split_input2 = foreach split_input GENERATE FLATTEN(STRSPLIT($0,'\\[')), FLATTEN(STRSPLIT($2,' ')), $3,  FLATTEN(STRSPLIT($5,'\\('));
split_input3 = FOREACH split_input2 GENERATE $1,$3,$6,$7,FLATTEN(STRSPLIT($5,'/',2));
rel_browser = FOREACH split_input3 GENERATE $0,$4,$1,$3,$5, FLATTEN(STRSPLIT($2,'/',2));
rel_browser_os = FOREACH rel_browser GENERATE FLATTEN(STRSPLIT($0,':',2)),$1,$2,$5,$4,FLATTEN(STRSPLIT($3,'\\;'));
rel_browser_os_time = FOREACH rel_browser_os GENERATE $2,$3,$4,$8,$0, FLATTEN(STRSPLIT($1,' ',2)), FLATTEN(STRSPLIT($5,'search-keyword='));
result = FOREACH rel_browser_os_time GENERATE $0,$1,$2,TRIM(REPLACE($3,'\\)','')),$4,$5,$8;
/* dump result; */
STORE result into '$output';
