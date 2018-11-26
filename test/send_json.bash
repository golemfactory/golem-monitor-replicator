#!/bin/bash

echo "Reading contents from $1"

cat $1|curl --header "Content-Type: application/json" --request POST --data @- -w "\nHTTP STATUS CODE: %{http_code}\n" http://localhost:8081
