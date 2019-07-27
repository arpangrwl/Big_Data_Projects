AccessLog = load '/user/ds503/project1/inputdata/AccessLog' using PigStorage(',')
as(AccessID:int, ByWho:int, WhatPage:int, TypeOfAccess:chararray, AccessTime:int);
Friends = load '/user/ds503/project1/inputdata/Friends' using PigStorage(',')
as(FriendRel:int, PersonID:int, MyFriend:int, DateofFriend:int, Desc:chararray);

AccessID1ID2 = foreach AccessLog generate ByWho as id1, WhatPage as id2;
FriendsID1ID2 = foreach Friends generate PersonID as id1, MyFriend as id2;

AccessID1ID2_d = distinct AccessID1ID2;
FriendsID1ID2_d = distinct FriendsID1ID2;

accessGroup = group AccessID1ID2_d by id1;
friendsGroup = group FriendsID1ID2_d by id1;

joined = join accessGroup by group, friendsGroup by group;


loseInterest = foreach joined{
  diff = SUBTRACT(FriendsID1ID2_d,AccessID1ID2_d);
  generate friendsGroup::group as id, COUNT(friendsGroup::FriendsID1ID2_d) as friendsNum, COUNT(diff) as loseInterestNum;
};

result = filter loseInterest by loseInterestNum > 0;


store result into '/user/ds503/project1/TaskG_pig';
