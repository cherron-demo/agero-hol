
use role roleXXX;
use warehouse whXXX;
use database hol;
use schema schemaXXX;

SELECT * FROM app_reviews;

select review, snowflake.cortex.sentiment(review) from app_reviews;

select review, snowflake.cortex.summarize(review) as my_summary
from app_reviews;

SET prompt = 
'### 
Put the sentiment as positive or negative and summary in json format. Keep the summary to 15 words or less. Only return the JSON object. Nothing after.
###';

select snowflake.cortex.complete('llama2-70b-chat',concat('[INST]',$prompt,review,'[/INST]')) as summary
from app_reviews ;

select snowflake.cortex.complete('mixtral-8x7b',concat('[INST]',$prompt,review,'[/INST]')) as summary
from app_reviews ;

select snowflake.cortex.complete('mistral-7b',concat('[INST]',$prompt,review,'[/INST]')) as summary
from app_reviews ;

select snowflake.cortex.complete('gemma-7b',concat('[INST]',$prompt,review,'[/INST]')) as summary
from app_reviews ;

