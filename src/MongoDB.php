<?php

namespace Soupmix\Adapters;

class MongoDB {
    
    public $conn = null;
    
    private $db_name = null;
    
    public $db = null;

    public function __construct($config){
        $this->db_name = $config['db_name'];
        $this->connect($config);
    }
    
    private function connect($config){
        $this->conn = new \MongoDB\Client($config['connection_string'], $config['options']);
        $this->db =$this->conn->{$this->db_name};
    }
    
    public function create($collection, $config){
        return $this->db->createCollection($collection);
    }
    
    public function drop($collection, $config){
        return $this->db->dropCollection($collection);
    }

    public function truncate($collection, $config){
        
        $this->db->dropCollection($collection);
        return $this->db->createCollection($collection);
    }
    
    public function create_indexes($collection, $indexes){
        $collection = $this->db->selectCollection($collection);
        return $collection->createIndexes($indexes);
    }
    
    public function insert($collection, $values){
        
        $collection = $this->db->selectCollection($collection);
        $result = $collection->insertOne($values);
        $id = $result->getInsertedId();
        if(is_object($id)){
            return (string) $id;
        }
        else{
            return null;
        }
    }

    public function get($collection, $id){
        
        $collection = $this->db->selectCollection($collection);
        $filter = ['_id'=> new \MongoDB\BSON\ObjectID($id)];
        $options = [
            'typeMap'=>['root' => 'array', 'document' => 'array']
        ];
        $result = $collection->findOne($filter, $options);
        if($result!==null){
            $result['id'] = (string) $result['_id'];
            unset($result['_id']);
        }
        return $result;
    }

    public function update($collection, $filter, $values){
        $collection = $this->db->selectCollection($collection);
        $filter = MongoDB::build_filter($filter)[0];
        $values_set = [ '$set' => $values ];
        if(isset($filter['id'])){
            $filter['_id'] = new \MongoDB\BSON\ObjectID($filter['id']);
            unset($filter['id']);
        }
        $result = $collection->updateMany( $filter, $values_set );
        return $result->getModifiedCount();
    }

    public function delete($collection, $filter){
        
        $collection = $this->db->selectCollection($collection);
        $filter = MongoDB::build_filter($filter)[0];
        if(isset($filter['id'])){
            $filter['_id'] = new \MongoDB\BSON\ObjectID($filter['id']);
            unset($filter['id']);
        }
        $result = $collection->deleteMany($filter);
        return $result->getDeletedCount();
    }

    public function find($collection, $filter, $fields=null, $sort=null, $start=0, $limit=25, $debug=false){
        
        $collection = $this->db->selectCollection($collection);
        if(isset($filter['id'])){
            $filter['_id'] = new \MongoDB\BSON\ObjectID($filter['id']);
            unset($filter['id']);
        }
        if($filter != null) {
            $filter = ['$and' => MongoDB::build_filter($filter)];
        }
        else{
            $filter = [];
        }
        $count = $collection->count($filter);
        if($count > 0){
            $results =[];
            $options=[
                'limit'    => (int) $limit,
                'skip'    => (int) $start,
                'typeMap'=>['root' => 'array', 'document' => 'array']
            ];
            if($fields !== null){
                $projection = [];
                foreach ($fields as $field){
                    if($field=='id'){
                        $field = '_id';
                    }
                    $projection[$field]=true;
                }
                $options['projection']=$projection;
            }
            if($sort !== null){
                foreach ($sort as $sort_key=> $sort_dir){
                    $sort[$sort_key]=($sort_dir=="desc")?-1:1;
                    if($sort_key == 'id'){
                        $sort['_id'] = $sort[$sort_key];
                        unset($sort['id']);
                    }
                }
                $options['sort']=$sort;
            }
            $cursor = $collection->find($filter,$options);
            $iterator = new \IteratorIterator($cursor);
            $iterator->rewind();
            while($doc = $iterator->current()){
                if(isset($doc['_id'])){
                    $doc['id'] = (string) $doc['_id'];
                    unset($doc['_id']);
                }
                 $results[]=$doc;
                 $iterator->next();
            }
            return ['total' => $count, 'data' => $results];
        }
        else{
            return ['total' => 0, 'data' => null];
        }
    }
    
    public function query($query){
        // reserved        
    }

    private static function build_filter($filter){

        $filters = [];
        $previous_key = "";
        foreach ($filter as $key=>$value){
            if(strpos($key,"__")!==false){
                preg_match('/__(.*?)$/i',$key, $matches );
                $operator = $matches[1];
                
                switch ($operator){
                    case '!in':
                        $operator = 'nin';
                        break;
                    case 'not':
                        $operator = 'ne';
                        break;
                    case 'wildcard':
                        $operator = 'regex';
                        $value = str_replace(array("?"),array("."),$value);
                        break;
                    case 'prefix':
                        $operator = 'regex';
                        $value = $value."*";    
                        break;
                }
                $key = str_replace($matches[0], "", $key);
                
                $filters[]=[$key=>['$'.$operator=>$value ]];
            }
            else if(strpos($key,"__")===false && is_array($value)){
                $filters[]['$or'] = MongoDB::build_filter($value);
            }
            else{
                $filters[][$key] = $value;
            }
            $previous_key = $key;
        }
        
        return $filters;
    }
}