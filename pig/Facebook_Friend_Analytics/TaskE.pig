AccessLog = load '/user/ds503/project1/inputdata/AccessLog' using PigStorage(',')
as(AccessID:int, ByWho:int, WhatPage:int, TypeOfAccess:chararray, AccessTime:int);


id1AccessID2 = foreach AccessLog generate ByWho, WhatPage;
distinctID1AccessID2 = distinct id1AccessID2;


group1 = group id1AccessID2 by ByWho;
accessCount = foreach group1 generate group as id, COUNT(id1AccessID2) as count;


group2 = group distinctID1AccessID2 by ByWho;
accessDistinctCount = foreach group2 generate group as id, COUNT(distinctID1AccessID2) as countDistinct;

joined = join accessCount by id, accessDistinctCount by id;

result = foreach joined generate accessDistinctCount::id as id, count, countDistinct;
result_ordered = order result by id;

store result_ordered into '/user/ds503/project1/TaskE_pig';
