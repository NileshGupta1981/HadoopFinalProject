REGISTER /usr/local/pig/lib/piggybank.jar;
REGISTER /home/acadgild/FilterObjective.jar;

DEFINE XPath org.apache.pig.piggybank.evaluation.xml.XPath();

sourcexml =  LOAD '/flume_import/*' using org.apache.pig.piggybank.storage.XMLLoader('row') as (x:chararray);

sourceparse = FOREACH sourcexml GENERATE TRIM(XPath(x, 'row/State_Name')) AS State_Name,
    TRIM(XPath(x, 'row/District_Name')) AS District_Name,
    TRIM(XPath(x, 'row/Project_Objectives_IHHL_BPL')) AS Project_Objectives_IHHL_BPL,
    TRIM(XPath(x, 'row/Project_Objectives_IHHL_APL')) AS Project_Objectives_IHHL_APL,
    TRIM(XPath(x, 'row/Project_Objectives_IHHL_TOTAL')) AS Project_Objectives_IHHL_TOTAL,
    TRIM(XPath(x, 'row/Project_Objectives_SCW')) AS Project_Objectives_SCW,
    TRIM(XPath(x, 'row/Project_Objectives_School_Toilets')) AS Project_Objectives_School_Toilets,
    TRIM(XPath(x, 'row/Project_Objectives_Anganwadi_Toilets')) AS Project_Objectives_Anganwadi_Toilets,
    TRIM(XPath(x, 'row/Project_Objectives_RSM')) AS Project_Objectives_RSM,
    TRIM(XPath(x, 'row/Project_Objectives_PC')) AS Project_Objectives_PC,
    TRIM(XPath(x, 'row/Project_Performance-IHHL_BPL')) AS Project_Performance_IHHL_BPL,
    TRIM(XPath(x, 'row/Project_Performance-IHHL_APL')) AS Project_Performance_IHHL_APL,
    TRIM(XPath(x, 'row/Project_Performance-IHHL_TOTAL')) AS Project_Performance_IHHL_TOTAL,
    TRIM(XPath(x, 'row/Project_Performance-SCW')) AS Project_Performance_SCW,
    TRIM(XPath(x, 'row/Project_Performance-School_Toilets')) AS Project_Performance_School_Toilets,
    TRIM(XPath(x, 'row/Project_Performance-Anganwadi_Toilets')) AS Project_Performance_Anganwadi_Toilets,
    TRIM(XPath(x, 'row/Project_Performance-RSM')) AS Project_Performance_RSM,
    TRIM(XPath(x, 'row/Project_Performance-PC')) AS Project_Performance_PC;

Fullobjective = filter sourceparse by ((Project_Objectives_IHHL_TOTAL <= Project_Performance_IHHL_TOTAL) AND (Project_Objectives_School_Toilets <= Project_Performance_School_Toilets) AND (Project_Objectives_SCW <= Project_Performance_SCW) AND (Project_Objectives_Anganwadi_Toilets <= Project_Performance_Anganwadi_Toilets) AND (Project_Objectives_RSM <= Project_Performance_RSM) AND (Project_Objectives_PC <= Project_Performance_PC) );

STORE Fullobjective into '/user/acadgild/Fullobjective' using PigStorage(',');

Targetobjective = filter sourceparse by PigUDF.FilterObjective(Project_Objectives_IHHL_TOTAL,Project_Performance_IHHL_TOTAL,Project_Objectives_School_Toilets, Project_Performance_School_Toilets,Project_Objectives_SCW,Project_Performance_SCW,Project_Objectives_Anganwadi_Toilets,
Project_Performance_Anganwadi_Toilets,Project_Objectives_RSM,Project_Performance_RSM,
Project_Objectives_PC,Project_Performance_PC);

