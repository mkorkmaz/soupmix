<?php


namespace Soupmix\Adapters;


class MongoDB {
	
	public $conn = null;
	
	private $db_name = null;
	
	private $db = null;

	public function __construct($config){
		$this->db_name = $config['db_name'];
		$this->connect($config);
	}
	
	private function connect($config){
		$this->conn = new \MongoDB\Client($config['connection_string'], $config['options']);
		$this->db =$this->conn->{$this->db_name};
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
			$result['_id'] = (string) $result['_id'];
		}
		return $result;
	}

	public function update($collection, $filter, $values){
		
		$collection = $this->db->selectCollection($collection);
		$filter = MongoDB::build_filter($filter);
		$values_set = [ '$set' => $values ];
		if(isset($filter['_id'])){
			$filter['_id'] = new \MongoDB\BSON\ObjectID($filter['_id']);
		}
		$result = $collection->updateMany( $filter, $values_set );
		return $result->getModifiedCount();
	}

	public function delete($collection, $filter){
		
		$collection = $this->db->selectCollection($collection);
		if(isset($filter['_id'])){
			$filter['_id'] = new \MongoDB\BSON\ObjectID($filter['_id']);
		}
		$result = $collection->deleteMany($filter);
		return $result->getDeletedCount();
	}

	public function find($collection, $filter, $fields=null, $sort=null, $start=0, $limit=25){
		
		$collection = $this->db->selectCollection($collection);
		if(isset($filter['_id'])){
			$filter['_id'] = new \MongoDB\BSON\ObjectID($filter['_id']);
		}
		$filter = ['$and'=>MongoDB::build_filter($filter)];
		
		$count = $collection->count($filter);
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
			$cursor = $collection->find($filter,$options);
			$iterator = new \IteratorIterator($cursor);
			$iterator->rewind();
			while($doc = $iterator->current()){
				if(isset($doc['_id'])){
					$doc['_id'] = (string) $doc['_id'];
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
					case 'not':
						$operator = 'ne';
					break;case '!in':
						$operator = 'nin';
						break;
				}
				
				
				$key = str_replace($matches[0], "", $key);
				
				
				$filters[]=[$key=>['$'.$operator=>$value]];
				
				
				
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