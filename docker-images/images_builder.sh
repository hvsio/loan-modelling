docker build -f ./docker-images/Dockerfile_data_modifier . -t h4sio/data_modifier3.1:1.0.3
docker build -f ./docker-images/Dockerfile_dataset_puller . -t h4sio/dataset_puller3.1:1.0.3
docker build -f ./docker-images/Dockerfile_database_creator . -t h4sio/database_creator3.1:1.0.3