# data-society takehome

## 0) Prerequisites:
- docker

## 1) Setup:
- Run the following commands to build and run the db and etl containers.
```console
docker compose -f docker-compose.yml build
docker compose -f docker-compose.yml up -d
```

## 2) ETL:
- Run the below command to initiate the ETL job.
```console
docker exec data-society-etl bash -c '. ~/.zshrc && cd $HOME && python jobs/pull_load_raw.py --start_year 2019 --end_year 2024'
```

## 3) Discussion:
Given more time, I would normalize the data using a star schema.