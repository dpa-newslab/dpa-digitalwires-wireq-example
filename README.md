# Import dpa-digitalwires via wireQ-API

This repository shows best practices of how to import the dpa-digitalwires
format via wireQ-API and store the received articles to the local file system.

There are two approaches:
1. [Fast and efficient retrieval of all new messages via POST request `dequeue_entries.json`](#dequeue)
2. [Fast and efficient retrieval of all new messages via GET and DELETE request](#get-and-delete)

## Requirements

This setup was tested with:

* >=Python3.8

## How to receive articles in dpa-digitalwires via wireQ-API

The python implementation can be found in `how_to_receive_wireq.py`.
This is basically an executable test which shows how to receive articles in the
dpa-digitalwires format via wireq-API.

Open your terminal and type:
```
export BASE_URL=https://...
python how_to_receive_wireq.py
```

## DEQUEUE

The python implementation can be found in `dequeue_import.py`.
Please copy the `base_url` from your setup in the [API-Portal](https://api-portal.dpa-newslab.com).
In the given example code the environment variable `BASE_URL` is used.

Open your terminal and type:
```
export BASE_URL=https://...
python dequeue_import.py
```

## GET and DELETE

The python implementation can be found in `get_delete_import.py`.
Please copy the `base_url` from your setup in the [API-Portal](https://api-portal.dpa-newslab.com).
In the given example code the environment variable `BASE_URL` is used.

Open your terminal and type:
```
export BASE_URL=https://...
python get_delete_import.py
```
