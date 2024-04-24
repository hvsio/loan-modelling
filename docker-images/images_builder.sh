docker build -f ./docker-images/database_creator/Dockerfile . -t h4sio/database_creator3.1:1.0.2
docker build -f ./docker-images/dataset_puller/Dockerfile . -t h4sio/dataset_puller3.1:1.0.2
docker build -f ./docker-images/data_modifier/Dockerfile . -t h4sio/data_modifier3.1:1.0.2

docker run --name database_creator_worker h4sio/database_creator3.1:1.0.2
docker run --name dataset_puller_worker h4sio/dataset_puller3.1:1.0.2
docker run --name data_modifier_worker h4sio/data_modifier3.1:1.0.2