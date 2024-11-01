
  create or replace   view dev.analytics.session_timestamp
  
   as (
    WITH session_data AS ( 
SELECT sessionId, ts 
FROM dev.raw_data.session_timestamp WHERE sessionId IS
NOT NULL ) SELECT * FROM session_data
  );

