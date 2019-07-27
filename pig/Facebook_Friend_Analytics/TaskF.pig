AccessLog = load '/user/ds503/project1/inputdata/AccessLog' using PigStorage(',')
as(AccessID:int, ByWho:int, WhatPage:int, TypeOfAccess:chararray, AccessTime:int);


group1 = group AccessLog by ByWho;
lastTimeAccess = foreach group1 generate group as id, MAX(AccessLog.AccessTime) as lastTime;

result = filter lastTimeAccess by lastTime < 900000;
result_ordered = order result by id;

store result into '/user/ds503/project1/TaskF_pig';
