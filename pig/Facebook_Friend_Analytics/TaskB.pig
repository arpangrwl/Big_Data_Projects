MyPage = load '/user/ds503/project1/inputdata/MyPage' using PigStorage(',')
as(ID:int, Name:chararray, Nationality:chararray,CountryCode:int, Hobby:chararray);

nationality_groups = group MyPage by Nationality;

resultB = foreach nationality_groups generate group, COUNT(MyPage.ID);

resultB_ordered = order resultB by group;

store resultB_ordered into '/user/ds503/project1/TaskB_pig';
