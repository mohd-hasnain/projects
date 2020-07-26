set mapreduce.reduce.memory.mb 8192;
set mapreduce.reduce.java.opts -Xmx7168m;

--extract CLM records
clm_raw = LOAD '/datalake/optum/optuminsight/t_dlz/tst/EDP_Cons/t_hdfs/CMS_837P_O_20171127232102.txt' AS (line:chararray);
-- clm_gheader_raw = LOAD '/datalake/optum/optuminsight/t_dlz/tst/EDP_Cons/t_hdfs/CMS_837P_O_20171127232102.txt' AS (line:chararray);
cas_raw = LOAD '/datalake/optum/optuminsight/t_dlz/tst/EDP_Cons/t_hdfs/CMS_837P_O_20171127232102.txt' AS (line:chararray);
clm_raw_rep = FOREACH clm_raw GENERATE REPLACE(REPLACE(line,'!',''),'^','') as line1;
clm_segmentized = FOREACH (GROUP clm_raw_rep ALL) GENERATE FLATTEN(TOKENIZE((BagToString(clm_raw_rep,'')),'~')) as l1;
-- clm_gheader_segmentized = FOREACH (GROUP clm_raw_rep ALL) GENERATE FLATTEN(TOKENIZE((BagToString(clm_raw_rep,'')),'~')) as l2;
clm_gheader_segments = FILTER clm_segmentized BY l1 MATCHES '"GS*HC*".*';
clm_gheader_records_split = FOREACH clm_gheader_segments GENERATE FLATTEN(STRSPLIT(l1, '\\*'));
clm_segments = FILTER clm_segmentized BY l1 MATCHES '("ST*837*"|"BHT*0019*"|"HL*"|"CLM*"|"HI*ABK:"|"HI*BK:"|"MOA*"|"LX*"|"SV1*").*';
clm_delimited = FOREACH clm_segments GENERATE ($0 MATCHES 'ST.*' ? '~' : ($0 MATCHES 'HL.*' ? '!' : ($0 MATCHES 'LX.*' ? '^' : '*'))),$0;
clm_records_clm_tuple = FOREACH (GROUP clm_delimited ALL) GENERATE FLATTEN(TOKENIZE((BagToString(clm_delimited,'')),'~')) as lA;
clm_records_clm_tuple1 = FOREACH clm_records_clm_tuple GENERATE TOKENIZE(lA,'!') as bag1;
clm_records_clm_tuple2 = FOREACH clm_records_clm_tuple1 {inner0 = limit $0 1;generate flatten(inner0),bag1.$0;}
clm_records_clm_tuple3 = FOREACH clm_records_clm_tuple2 GENERATE $0,FLATTEN($1);
clm_records_clm_tuple4 = FILTER clm_records_clm_tuple3 BY $1 MATCHES '("HL*").*';
clm_records_tuple1 = FOREACH clm_records_clm_tuple4 GENERATE $0, TOKENIZE($1,'^') as bag2;
clm_records_tuple2 = FOREACH clm_records_tuple1 {inner1 = limit $1 1;generate $0, flatten(inner1),bag2.$0;}
clm_records_tuple3 = FOREACH clm_records_tuple2 GENERATE $0,$1,FLATTEN($2);
clm_records_tuple4 = FILTER clm_records_tuple3 BY $2 MATCHES '("LX*").*';
clm_records_split = FOREACH clm_records_tuple4 GENERATE FLATTEN(STRSPLIT($0, '\\*')),FLATTEN(STRSPLIT($1, '\\*')), FLATTEN(STRSPLIT($2, '\\*'));
SPLIT clm_records_split INTO 
	HI1 IF ($26 == 'HI'), 
	HI2 IF ($27 == 'HI'), 
	HI3 IF ($28 == 'HI'), 
	HIGood OTHERWISE;
HI1_Adj = FOREACH HI1 GENERATE ..$25,null,null,null,null,null,null,null,null,null,null,null,$26..;
HI2_Adj = FOREACH HI2 GENERATE ..$26,null,null,null,null,null,null,null,null,null,null,$27..;
HI3_Adj = FOREACH HI3 GENERATE ..$27,null,null,null,null,null,null,null,null,null,$28..;
HI_Adj = UNION HI1_Adj,HI2_Adj,HI3_Adj,HIGood;
SPLIT HI_Adj INTO 
	MOA1 IF ($39 == 'MOA'), 
	MOA2 IF ($40 == 'MOA'),
	MOA3 IF ($41 == 'MOA'),
	MOA4 IF ($42 == 'MOA'),
	MOA5 IF ($43 == 'MOA'),
	MOA6 IF ($44 == 'MOA'),
	MOA7 IF ($45 == 'MOA'),
	MOA8 IF ($46 == 'MOA'),
	MOA9 IF ($47 == 'MOA'),
	MOA10 IF ($48 == 'MOA'),
	MOA11 IF ($49 == 'MOA'),
	MOAGood OTHERWISE;	
MOA_Adj_1 = FOREACH MOA1 GENERATE ..$38,null,null,null,null,null,null,null,null,null,null,null,$39..;
MOA_Adj_2 = FOREACH MOA2 GENERATE ..$39,null,null,null,null,null,null,null,null,null,null,$40..;
MOA_Adj_3 = FOREACH MOA3 GENERATE ..$40,null,null,null,null,null,null,null,null,null,$41..;
MOA_Adj_4 = FOREACH MOA4 GENERATE ..$41,null,null,null,null,null,null,null,null,$42..;
MOA_Adj_5 = FOREACH MOA5 GENERATE ..$42,null,null,null,null,null,null,null,$43..;
MOA_Adj_6 = FOREACH MOA6 GENERATE ..$43,null,null,null,null,null,null,$44..;
MOA_Adj_7 = FOREACH MOA7 GENERATE ..$44,null,null,null,null,null,$45..;
MOA_Adj_8 = FOREACH MOA8 GENERATE ..$45,null,null,null,null,$46..;
MOA_Adj_9 = FOREACH MOA9 GENERATE ..$46,null,null,null,$47..;
MOA_Adj_10 = FOREACH MOA10 GENERATE ..$47,null,null,$48..;
MOA_Adj_11 = FOREACH MOA11 GENERATE ..$48,null,$49..;
MOA_Adj = UNION MOA_Adj_1,MOA_Adj_2,MOA_Adj_3,MOA_Adj_4,MOA_Adj_5,MOA_Adj_6,MOA_Adj_7,MOA_Adj_8,MOA_Adj_9,MOA_Adj_10,MOA_Adj_11;
SPLIT MOAGood INTO 
	LX1 IF ($39 == 'LX'), 
	LX2 IF ($40 == 'LX'),
	LX3 IF ($41 == 'LX'),
	LX4 IF ($42 == 'LX'),
	LX5 IF ($43 == 'LX'),
	LX6 IF ($44 == 'LX'),
	LX7 IF ($45 == 'LX'),
	LX8 IF ($46 == 'LX'),
	LX9 IF ($47 == 'LX'),
	LX10 IF ($48 == 'LX'),
	LX11 IF ($49 == 'LX'),
	LXGood OTHERWISE;	
LX_Adj_1 = FOREACH LX1 GENERATE ..$38,null,null,null,null,null,null,null,null,null,null,null,$39..;
LX_Adj_2 = FOREACH LX2 GENERATE ..$39,null,null,null,null,null,null,null,null,null,null,$40..;
LX_Adj_3 = FOREACH LX3 GENERATE ..$40,null,null,null,null,null,null,null,null,null,$41..;
LX_Adj_4 = FOREACH LX4 GENERATE ..$41,null,null,null,null,null,null,null,null,$42..;
LX_Adj_5 = FOREACH LX5 GENERATE ..$42,null,null,null,null,null,null,null,$43..;
LX_Adj_6 = FOREACH LX6 GENERATE ..$43,null,null,null,null,null,null,$44..;
LX_Adj_7 = FOREACH LX7 GENERATE ..$44,null,null,null,null,null,$45..;
LX_Adj_8 = FOREACH LX8 GENERATE ..$45,null,null,null,null,$46..;
LX_Adj_9 = FOREACH LX9 GENERATE ..$46,null,null,null,$47..;
LX_Adj_10 = FOREACH LX10 GENERATE ..$47,null,null,$48..;
LX_Adj_11 = FOREACH LX11 GENERATE ..$48,null,$49..;
LX_Adj = UNION LX_Adj_1,LX_Adj_2,LX_Adj_3,LX_Adj_4,LX_Adj_5,LX_Adj_6,LX_Adj_7,LX_Adj_8,LX_Adj_9,LX_Adj_10,LX_Adj_11,LXGood;
MOA_LX_Adj = UNION MOA_Adj,LX_Adj;
SPLIT MOA_LX_Adj INTO A1 IF ($50 == 'LX'), 
	  ALX IF ($50 == 'MOA' AND $55 == 'LX'),
	  BLX IF ($50 == 'MOA' AND $59 == 'LX'), 
	  CLX IF($50 == 'MOA' AND $56 == 'LX'),
	  DLX IF($50 == 'MOA' AND $57 == 'LX'), 
	  ELX IF($50 == 'MOA' AND $58 == 'LX'),
	  FLX IF($50 == 'MOA' AND $60 == 'LX'),
	  GLX IF($50 == 'MOA' AND $53 == 'LX'),
	  HLX IF($50 == 'MOA' AND $52 == 'LX'),
	  AGood OTHERWISE;
AMOA = FOREACH A1 GENERATE ..$49,'MOA',null,null,null,$50..;
ALX1 = FOREACH ALX GENERATE ..$53,$55..;
ALX2 = FOREACH ALX GENERATE ..$52,$54..;
BLX1 = FOREACH BLX GENERATE ..$53,$59..;
BLX2 = FOREACH BLX GENERATE ..$52,$54,$59..;
BLX3 = FOREACH BLX GENERATE ..$52,$55,$59..;
BLX4 = FOREACH BLX GENERATE ..$52,$56,$59..;
BLX5 = FOREACH BLX GENERATE ..$52,$57,$59..;
CLX1 = FOREACH CLX GENERATE ..$53,$56..;
CLX2 = FOREACH CLX GENERATE ..$52,$54,$56..;
CLX3 = FOREACH CLX GENERATE ..$52,$55,$56..;
DLX1 = FOREACH DLX GENERATE ..$52,$53,$57..;
DLX2 = FOREACH DLX GENERATE ..$52,$54,$57..;
DLX3 = FOREACH DLX GENERATE ..$52,$55,$57..;
DLX4 = FOREACH DLX GENERATE ..$52,$56,$57..;
ELX1 = FOREACH ELX GENERATE ..$52,$53,$58..;
ELX2 = FOREACH ELX GENERATE ..$52,$54,$58..;
ELX3 = FOREACH ELX GENERATE ..$52,$55,$58..;
ELX4 = FOREACH ELX GENERATE ..$52,$56,$58..;
ELX5 = FOREACH ELX GENERATE ..$52,$57,$58..;
FLX1 = FOREACH FLX GENERATE ..$52,$53,$60..;
FLX2 = FOREACH FLX GENERATE ..$52,$54,$60..;
FLX3 = FOREACH FLX GENERATE ..$52,$55,$60..;
FLX4 = FOREACH FLX GENERATE ..$52,$56,$60..;
FLX5 = FOREACH FLX GENERATE ..$52,$57,$60..;
GLX1 = FOREACH GLX GENERATE ..$52,null,$53..;
HLX1 = FOREACH HLX GENERATE ..$51,null,null,$52..;
B = UNION AGood,ALX1,ALX2,BLX1,BLX2,BLX3,BLX4,BLX5,AMOA,CLX1,CLX2,CLX3,DLX1,DLX2,DLX3,DLX4,ELX1,ELX2,ELX3,ELX4,ELX5,FLX1,FLX2,FLX3,FLX4,FLX5,GLX1,HLX1;
BB = FOREACH B GENERATE .. $68;
BB_dist = distinct BB;
--extract CAS records
cas_segmentized = FOREACH (GROUP cas_raw ALL) GENERATE FLATTEN(TOKENIZE((BagToString(cas_raw,'')),'~')) as l1;
cas_receiver_segments = FILTER cas_segmentized BY l1 MATCHES '("ST*837*"|"LX*"|"DTP*472*D8"|"DTP*472*RD8"|"CAS*").*';
cas_receiver_delimited = FOREACH cas_receiver_segments GENERATE ($0 MATCHES 'ST.*' ? '~' : ($0 MATCHES 'LX.*' ? '$' : ($0 MATCHES 'CAS.*' ? '^' : '*'))),$0;
cas_receiver_records_tuple = FOREACH (GROUP cas_receiver_delimited ALL) GENERATE FLATTEN(TOKENIZE((BagToString(cas_receiver_delimited,'')),'~')) as lA;
cas_receiver_records_tuple1 = FOREACH cas_receiver_records_tuple GENERATE TOKENIZE(lA,'$') as bag1;
cas_receiver_records_tuple2 = FOREACH cas_receiver_records_tuple1 {inner1 = limit $0 1;generate flatten(inner1),bag1.$0;}
cas_records_sv_cas = FOREACH cas_receiver_records_tuple2 GENERATE REPLACE($0, '\\^.*',''), $1;
cas_receiver_records_tuple3 = FOREACH cas_records_sv_cas GENERATE $0,FLATTEN($1);
cas_records_sv_cas_false = FILTER cas_receiver_records_tuple3 BY $1 MATCHES '("LX*").*' AND NOT $1 MATCHES '.*CAS.*';
cas_records_sv_cas_true = FILTER cas_receiver_records_tuple3 BY $1 MATCHES '("LX*").*' AND $1 MATCHES '.*CAS.*';
cas_receiver_records_tuple4 = FOREACH cas_records_sv_cas_true GENERATE $0,TOKENIZE($1, '^') as iB;
--make st and lx records as the keys for cas
t_sample = FOREACH cas_receiver_records_tuple4 {inner0 = $0; inner1 = limit $1 1;generate inner0,flatten(inner1), iB.$0;}
t_sample1 = FOREACH t_sample GENERATE $0,$1,FLATTEN($2);
--since above tuple also has LX as token in the bag, flattening will result in creating $0 = ST* $1= LX*1 $2 = LX*1 kinda records
cas_receiver_records_tuple5 = FILTER t_sample1 BY $1 MATCHES '("LX*").*' AND NOT $2 MATCHES '("LX*").*';
cas_records_sv_cas_true_split = FOREACH cas_receiver_records_tuple5 GENERATE FLATTEN(STRSPLIT($0, '\\*')),FLATTEN(STRSPLIT($1, '\\*')), FLATTEN(STRSPLIT($2, '\\*'));
cas_records_sv_cas_false_split = FOREACH cas_records_sv_cas_false GENERATE FLATTEN(STRSPLIT($0, '\\*')),FLATTEN(STRSPLIT($1, '\\*'));
SPLIT cas_records_sv_cas_true_split INTO CASAGood IF ($15 IS NULL), CASA OTHERWISE;
CASA1 = FOREACH CASA GENERATE ..$14;
CASA2 = FOREACH CASA GENERATE ..$11,$15..;
CASB = UNION CASAGood,CASA1,CASA2;
SPLIT CASB INTO CASBGood IF ($15 IS NULL), CASB OTHERWISE;
CASB1 = FOREACH CASB GENERATE ..$14;
CASB2 = FOREACH CASB GENERATE ..$11,$15..;
CASC = UNION CASBGood,CASB1,CASB2;
SPLIT CASC INTO CASCGood IF ($15 IS NULL), CASC OTHERWISE;
CASC1 = FOREACH CASC GENERATE ..$14;
CASC2 = FOREACH CASC GENERATE ..$11,$15..;
CASD = UNION CASCGood,CASC1,CASC2;
SPLIT CASD INTO CASDGood IF ($15 IS NULL), CASD OTHERWISE;
CASD1 = FOREACH CASD GENERATE ..$14;
CASD2 = FOREACH CASD GENERATE ..$11,$15,$16,$17;
CASE1 = UNION CASDGood,CASD1,CASD2;
SPLIT CASE1 INTO CASF1 IF ($14 IS NULL), CASF2 OTHERWISE;
CASF3 = FOREACH CASF1 GENERATE ..$13,null;
CASF = UNION CASF3,CASF2,cas_records_sv_cas_false_split;
--Join CLM records and CAS records by record no and service line no
X = JOIN BB_dist BY ($2, $55),CASF BY ($2,$5);
final = CROSS clm_gheader_records_split, X;
final_selected = FOREACH final GENERATE $2,$3,$6,$11,$12,$15,$16,$17,$18,$19,'1','2','2',$21,$22,$25,$27,$28,$29,$40,$42,$55,$56,$57,$59,$61,$62,$63,$64,$75,$76,$77,$79,$80,$81,'CH';
STORE final_selected INTO 'scratch/837P/claim/CH_837P_O_20171026221016/final_selected' USING PigStorage('\u0001');