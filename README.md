## Dapi

Dapi introspects your database and creates CRUD endpoints. All data from your tables is available at ```localhost:9000/api/v1/crud/<:table>``` and you can specify search criteria with ```?col1=val1&col2=val2```. Special search terms include limit and offset.

Sample Call:

```
# using http (pip install httpie)
$ http GET :9000/api/v1/crud/user limit==1 offset==2
```

```
# using curl
$ curl -X GET 'localhost:9000/api/v1/crud/user?limit=1&offset=2'
```

Response:

```
HTTP/1.1 200 OK
Content-Length: 50
Content-Type: application/json
Date: Sat, 14 Jun 2014 19:39:32 GMT

[
    {
        "email": "c@example.com",
        "id": "3",
        "name": "john"
    }
]
```