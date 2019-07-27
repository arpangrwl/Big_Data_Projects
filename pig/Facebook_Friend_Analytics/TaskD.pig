MyPage = load '/user/ds503/project1/inputdata/MyPage' using PigStorage(',')
as(ID:int, Name:chararray, Nationality:chararray,CountryCode:int, Hobby:chararray);

Friends = load '/user/ds503/project1/inputdata/Friends' using PigStorage(',')
as(FriendRel:int, PersonID:int, MyFriend:int, DateofFriend:int, Desc:chararray);

id1 = foreach Friends generate PersonID as id;
id2 = foreach Friends generate MyFriend as id;
idTotal = union id1,id2;

id_groups = group idTotal by id;
friendCount = foreach id_groups generate group as id, COUNT(idTotal) as count;

friendCount_ordered = order friendCount by id;

joined = join MyPage by ID, friendCount_ordered by id;

result = foreach joined generate Name, id, count;

store result into '/user/ds503/project1/TaskD_pig';
