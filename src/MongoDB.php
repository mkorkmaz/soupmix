<?php

/*
 * 
 *	TODO: Documentation 
 */


namespace DataStore\Adapters;


class MongoDB {
	
	private $conn = null;

	public function __construct($config){
		$this->connect($config);
	}
	
	private function connect($config){
		$this->conn = new \MongoDB\Client($config['connection_string'], $config['options']);
	}
	
	public function insert($collection, $values){
		
		list($dbname, $collection) = explode("/", $collection);
		$result = $this->conn->$dbname->$collection->insertOne($values);
		return $result->getInsertedId();
	}

	public function get($collection, $id){
		
		list($dbname, $collection) = explode("/", $collection);
		$filter = ['_id'=> new \MongoDB\BSON\ObjectID($id)];
		$options = [
			'typeMap'=>['root' => 'array', 'document' => 'array']
		];
		return $this->conn->$dbname->$collection->findOne( $filter,$options );
	}

	public function update($collection, $filter, $values){
	
		list($dbname ,$collection) = explode("/", $collection);
		$values_set = [ '$set' => $values ];
		if(isset($filter['_id'])){
			$filter['_id'] = new \MongoDB\BSON\ObjectID($filter['_id']);
		}
		return $this->conn->$dbname->$collection->updateMany( $filter, $values_set );
	}

	public function delete($collection, $filter){
		
		list($dbname, $collection) = explode("/", $collection);
		if(isset($filter['_id'])){
			$filter['_id'] = new \MongoDB\BSON\ObjectID($filter['_id']);
		}
		return $this->conn->$dbname->$collection->deleteMany( $filter );
	}

	public function find($collection, $filter, $fields=null, $sort=null, $start=0, $limit=25){

		list($dbname, $collection) = explode("/",$collection);		
		if(isset($filter['_id'])){
			$filter['_id'] = new \MongoDB\BSON\ObjectID($filter['_id']);
		}
		$filter = MongoDB::build_filter($filter);
		$count = $this->conn->$dbname->$collection->count($filter);
		if($count > 0){
			$results =[];
			$options=[
				'limit'	=> (int) $limit,
				'skip'	=> (int) $start,
				'typeMap'=>['root' => 'array', 'document' => 'array']
			];
			if($fields !== null){
				$projection = [];
				foreach ($fields as $field){
					$projection[$field]=true;
				}
				$options['projection']=$projection;
			}
			if($sort !== null){
				foreach ($sort as $sort_key=> $sort_dir){
					$sort[$sort_key]=($sort_dir=="desc")?-1:1;
				}
				$options['sort']=$sort;
			}
			
			$cursor = $this->conn->$dbname->$collection->find($filter,$options);
			$iterator = new \IteratorIterator($cursor);
			$iterator->rewind();
			while($doc = $iterator->current()){
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
		foreach ($filter as $key=>$value){
			if(strpos($key,"__")!==FALSE){
				preg_match('/__(.*?)$/i',$key, $matches );
				$operator = $matches[1];
				$key = str_replace($matches[0], "", $key);
				$filters[$key]=[ '$'.$operator => $value];
			}
			else{
				$filters[$key] = $value;
			}
		}
		return $filters;
	}
}