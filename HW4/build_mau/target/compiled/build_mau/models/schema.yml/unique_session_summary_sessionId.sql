
    
    

select
    sessionId as unique_field,
    count(*) as n_records

from DEMO_DB.analytics.session_summary
where sessionId is not null
group by sessionId
having count(*) > 1


