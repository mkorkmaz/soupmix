# Soupmix

## Connect

### MongoDB

MongoDD Adapter is based on mongodb extension and [mongo-php-library](https://github.com/mongodb/mongo-php-library). It uses simple configuration that includes db_name, mongoDB connection string and connection options that needed by **mongo-php-library's MongoDB\Client**. You can read more details about connection string and connection options on [mongo-php-library documentaiton](http://mongodb.github.io/mongo-php-library/classes/client/)

```
// Connect to MongoDB Service
$adapter_config = [];
$adapter_config['db_name'] ='db_name';
$adapter_config['connection_string']="mongodb://127.0.0.1";
$adapter_config['options'] =[];
$m=new Soupmix\Adapters\MongoDB($adapter_config);
```

### Elasticsearch

ElasticSearch Adaper is based on [Elastic](http://elastic.co)'s [elasticsearch-php library](https://github.com/elastic/elasticsearch-php). It uses simple configuration that includes db_name as index name in elasticsearch and host names encoded in an array. 

```
// Connect to Elasticsearch Service
$adapter_config 			= [];
$adapter_config['db_name'] 	= 'index_name';
$adapter_config['hosts']	= ["127.0.0.1:9200"];
$adapter_config['options'] 	= [];

$e=new Soupmix\Adapters\ElasticSearch($adapter_config);

```
## Insert


```
(mixed) insert( (str) $collection, (array) $values)
```
**$collection**: Collection name for MongoDB, _type name for Elasticsearch, table name for sql.

**$values**: Document defined as a valid array to be inserted.

returns null if failed, str value as id for MongoDB and Elasticsearch, and integer as primary key id for SQL.

**Important: This function does not validate or sanitise the values to be inserted. It assumes validation and sanitisation are already done.**


## Get By ID
```
(array|null) get( (str) $collection, (str) $id)
```
**$collection**: Collection name for MongoDB, _type name for Elasticsearch, table name for sql.

**$id**: any string value as document id. It converts id's variable type to Mongo Object ID for Mongo DB, to integer for SQL.

returns document as an array or null if document with this id does not exist.

## Delete
```
(int) delete( (str) $collection, (array) $filter)
```
**$collection**: Collection name for MongoDB, _type name for Elasticsearch, table name for sql.

**$filter**: Criteria that to determine documents that deletion operation will be on. 

returns number of documents that deleted.

## Update

```
(int) update( (str) $collection, (array) $filter, (array) $values)
```
**$collection**: Collection name for MongoDB, _type name for Elasticsearch, table name for sql.

**$filter**: Criteria that to determine documents that update operation will be on. 

**$values**: Partia or full document defined as a valid array to be updated

Returns number of documents that updated.


## Find
```
(array) find( (str) $collection, (array) $filter, (array) $fields=null, (array) $sort=null, (int) $start=0, (int) $limit=25)
```
**$collection**: Collection name for MongoDB, _type name for Elasticsearch, table name for sql.

**$filter**: Criteria that to determine documents to be find

**$fields**: Fields that will be included in returned result. If null, it will return full document.

**$sort**: Sorting criteria encoded as an array. Array key is field name and its value is that one of these: asc (ascending),desc (decending). Function converts asc and desc notation to 1,-1 notation for MongoDB.
```
$sort=['full_name'=>'asc','age'=>'desc'];
```
**$start**: To set the value for returning documents starting from the records that have been found. Default is 0 (zero)

**$limit**: To set how many documents will be returned according to criteria. Default is 25.


Returns an array that has to item: total and data. Total gives the number of documents that satisfies the criteria and data gives the returned documents that framed by $start and $limit parameters.

## Filter Operators

To satisfy querying options we use some special keywords that will be used in filter arrays. You can apply them just adding to field name with prefixing two underscore (ie: For the field 'age' you can use **'age__gte'**)

**gt**: It filters the field that has values greater than the value specified.

**gte**: It filters the field that has values greater than and equal to the value specified.

**lt**: It filters the field that has values lower than the value specified.

**lte**: It filters the field that has values lower than and equal to the value specified.

**not**: It filters the field that does not have the value specified.

**in**: It filters the field that has values specified as an array. For Elasticsearch, if the values are text, [this field must be defined not analysed](https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-terms-query.html).

**!in**: It filters the field that does not have values specified as an array. For Elasticsearch, if the values are text, [this field must be defined not analysed](https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-terms-query.html).


**wildchard**: It filters the field that has values that matches with the value specified. For multiple characters use * and for a single character use ? for the filter value. (ie: ['full\_name':"J*Doe"] filters the documents with the full\_name have John Doe, Jack Doe, Jane Doe. ['full\_name':"J?ck Doe"] filters the documents with the full_name has Jack Doe). It's "LIKE % " expression in SQL.






...