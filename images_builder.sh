docker build -f ./docker-images/database_creator/Dockerfile . -t database_creator
docker build -f ./docker-images/dataset_puller/Dockerfile . -t dataset_puller
docker build -f ./docker-images/data_modifier/Dockerfile . -t data_modifier

docker run --name database_creator_worker database_creator
docker run --name dataset_puller_worker dataset_puller
docker run --name data_modifier_worker data_modifier