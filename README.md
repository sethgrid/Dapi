## Dapi

Dapi introspects your database and creates CRUD endpoints. All data from your tables is available at ```localhost:9000/api/v1/crud/<:table>``` and you can specify search criteria with ```?col1=val1&col2=val2```. Special search terms include limit and offset. Additionally, Dapi provides _meta endpoints to enable discoverability.

### Meta Endpoints

To help know the columns and tables available via the API, you can use the ```_meta``` endpoints. ```.../crud/_meta``` will return the meta data for all of your tables, while ```.../crud/<:table>/_meta``` will only return the meta data for that table.

Note, the sample calls below are using HTTPie (pip install httpie). They work just as well with curl.

```
$ http GET :9000/api/v1/crud/user/_meta
HTTP/1.1 200 OK
Content-Length: 465
Content-Type: application/json
Date: Sat, 14 Jun 2014 23:01:41 GMT

{
    "description": "MySQL Table user",
    "location": "/api/v1/crud/user/",
    "methods": [
        "GET",
        "POST",
        "PUT",
        "DELETE"
    ],
    "primary": "id",
    "properties": {
        "email": {
            "description": "",
            "type": "varchar"
        },
        "id": {
            "description": "",
            "type": "int"
        },
        "limit": {
            "description": "Used to limit the number of results returned",
            "type": "int"
        },
        "name": {
            "description": "",
            "type": "varchar"
        },
        "offset": {
            "description": "Used to offset results returned",
            "type": "int"
        }
    },
    "required": [
        "id"
    ],
    "title": "user",
    "type": "object"
}
```

#### Accessing Data

Dapi allows you to easily GET data from your MySQL database.

Sample Call:

Response:

```
# using curl
# $ curl -X GET 'localhost:9000/api/v1/crud/user?limit=1&offset=2'

# using httpie
$ http GET :9000/api/v1/crud/user limit==1 offset==2
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