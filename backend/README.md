# Backend for the Dashboard

*****************************

## Run the aggregation

```bash
bash aggregate.sh
```

*****************************

## Start the server

1. Development Mode

```bash
python3 manage.py runserver
```

2. Production Mode

Collect Static Files

```bash
python3 manage.py collectstatic
```

Deploy

```bash
sudo ./deploy.sh
```

*****************************