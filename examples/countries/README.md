# Euro currency countries example

In this example we use workload to fetch country information from public API https://restcountries.eu
and get number of countries (and country names) which use Euro currency.

You can use docker compose to build and run

```
docker-compose build --no-cache
docker-compose up
```

You can also play with number of workers to see if there is any performance speedup

```
docker-compose up --scale docker=3
```