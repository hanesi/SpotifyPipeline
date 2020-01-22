# SpotifyPipeline
Pipeline to retrieve spotify listening data and ETL into an RDS instance

Requires an access token from Spotify to run. 

Designed as a lambda function that runs every 2 hours to retrieve the last 50 played songs (API maximum). It then queries the database for the timestamp of the last played song and filters to avoid uploading duplicates.
