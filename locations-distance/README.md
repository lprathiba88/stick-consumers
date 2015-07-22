To run:

Set environment variables to connect to AWS/Kinesis, Firebase: AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, FIREBASE_SECRET, GMAP_API_KEY
GMAP_API_KEY is the server key for GeoCoding API.

mvn package

java -jar target/locations-distance-1.0-SNAPSHOT-jar-with-dependencies.jar