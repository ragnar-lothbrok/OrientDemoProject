select featuregroup_id,vertex.feature_id,vertex.feature_name from ( select out(contains).out() as vertex,out(contains).featuregroup_id as featuregroup_id from Role where role_id=1)


select parent.outE('FeatureGroupMapping').permission,featuregroup_id,vertex.feature_id,vertex.feature_name from ( select out(contains) as parent,out(contains).out() as vertex,out(contains).featuregroup_id as featuregroup_id from Role where role_id=1)


select parent.outE('FeatureGroupMapping').permission,parent.featuregroup_id,parent.out('FeatureGroupMapping') from ( select out(contains) as parent from Role where role_id=1);

select featuregroup_id,out('FeatureGroupMapping') ,outE('FeatureGroupMapping').permission from (traverse out from  ( select out(contains) as parent from Role where role_id=1))

select featuregroup_id,out('FeatureGroupMapping').include('feature_id','feature_name') ,outE('FeatureGroupMapping').permission from (traverse out from  ( select out(contains) as parent from Role where role_id=1))

select * from acl_group_user_mapping agcm inner join acl_group ag on ag.id=agcm.group_id inner join acl_user au on au.id=agcm.user_id limit 1;


<!-- New Queries-->

select featuregroup_id,out('FeatureGroupMapping').include('feature_id','feature_name') ,outE('FeatureGroupMapping').permission from (traverse out from  ( select out(contains) as parent from (traverse out from (select out('has') as role from Team where team_id ='team_21'))))


select expand(out('hasTeamChild')) from Team where team_id='team_pqr'

select featuregroup_id,out('FeatureGroupMapping').include('feature_id','feature_name') ,outE('FeatureGroupMapping').permission from (traverse out from  ( select out(contains) as parent from (traverse out from (select out('has') as role from (select * from (traverse out('hasTeamChild') from (select * from  Team where team_id='team_25')))))))


select team_id,out('has') from (traverse out('hasTeamChild') from (select * from  Team where team_id='team_25'))

traverse out('has') from (traverse out('hasTeamChild') from (select * from  Team where team_id='team_25'))

http://www.fromdev.com/2013/09/Gremlin-Example-Query-Snippets-Graph-DB.html