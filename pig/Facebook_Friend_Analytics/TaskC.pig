AccessLog = load '/user/ds503/project1/inputdata/AccessLog' using PigStorage(',')
as(AccessID:int, ByWho:int, WhatPage:int, TypeOfAccess:chararray, AccessTime:int);

WhatPage_groups = group AccessLog by WhatPage;

resultC = foreach WhatPage_groups generate group as id, COUNT(AccessLog.AccessID) as count;

resultC_ordered = order resultC by count desc;

top10 = limit resultC_ordered 10;

store top10 into '/user/ds503/project1/TaskC_pig';
