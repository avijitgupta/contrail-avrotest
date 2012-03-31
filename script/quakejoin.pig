fq1 = LOAD 'ContrailPlus/Quake_Mate_1_Flat' AS (seq_id:chararray, kmer:chararray, qv:chararray);
fq2 = LOAD 'ContrailPlus/Quake_Mate_2_Flat' AS (seq_id:chararray, kmer:chararray, qv:chararray);

fq1_filtered = FOREACH fq1 GENERATE $0, $1, $2;
fq2_filtered = FOREACH fq2 GENERATE $0, $1, $2;

joinedfq = JOIN fq1_filtered BY $0, fq2_filtered BY $0 USING 'merge';

STORE joinedfq INTO 'ContrailPlus/Quake_Join_Out'; 
