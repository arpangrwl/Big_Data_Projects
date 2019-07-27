Friends = load '/user/ds503/project1/inputdata/Friends' using PigStorage(',')
as(FriendRel:int, PersonID:int, MyFriend:int, DateofFriend:int, Desc:chararray);

id1 = foreach Friends generate PersonID as id;
id2 = foreach Friends generate MyFriend as id;
idTotal = union id1,id2;

id_groups = group idTotal by id;
friendCount = foreach id_groups generate group as id, COUNT(idTotal) as count;

friendCountGroup = group friendCount all;
totalAvg = foreach friendCountGroup generate AVG(friendCount.count) as avg;

friendCount_filtered = filter friendCount by count > totalAvg.avg;
result = foreach friendCount_filtered generate id, count, totalAvg.avg as totalAvg;
result_order = order result by id;

store result_order into '/user/ds503/project1/TaskH_pig';
