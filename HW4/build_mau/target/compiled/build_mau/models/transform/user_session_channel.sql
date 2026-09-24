SELECT
    userId,
    sessionId,
    channel
FROM DEMO_DB.raw.user_session_channel
WHERE sessionId IS NOT NULL