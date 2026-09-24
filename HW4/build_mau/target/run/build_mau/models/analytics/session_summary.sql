
  
    

create or replace transient table DEMO_DB.analytics.session_summary
    
    
    
    
    

    as (with __dbt__cte__user_session_channel as (
SELECT
    userId,
    sessionId,
    channel
FROM DEMO_DB.raw.user_session_channel
WHERE sessionId IS NOT NULL
),  __dbt__cte__session_timestamp as (
SELECT
    sessionId,
    ts
FROM DEMO_DB.raw.session_timestamp
WHERE sessionId IS NOT NULL
) SELECT u.userId, u.sessionId, u.channel, st.ts
FROM __dbt__cte__user_session_channel u
JOIN __dbt__cte__session_timestamp st ON u.sessionId = st.sessionId
    )
;


  