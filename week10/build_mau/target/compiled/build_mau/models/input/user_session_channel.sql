WITH user_channel_data AS ( SELECT userId, sessionId, channel
FROM dev.raw_data.user_session_channel WHERE
sessionId IS NOT NULL ) SELECT * FROM user_channel_data