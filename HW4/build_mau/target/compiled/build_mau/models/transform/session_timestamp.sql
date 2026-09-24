SELECT
    sessionId,
    ts
FROM DEMO_DB.raw.session_timestamp
WHERE sessionId IS NOT NULL