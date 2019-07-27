MyPage = load '/user/ds503/project1/inputdata/MyPage' using PigStorage(',')
as(ID:int, Name:chararray, Nationality:chararray,CountryCode:int, Hobby:chararray);

MyPage_filtered = filter MyPage by Nationality matches 'Chinese';

store MyPage_filtered into '/user/ds503/project1/TaskA_pig';
