<?php

/*
 * 
 *	TODO: Documentation 
 */


namespace DataStore\Adapters;


class ElasticSearch {
	
	private $conn = null;
	
	private $index = null;

	public function __construct($config){
		$this->index = $config['db_name'];
		$this->connect($config);
	}
	
	private function connect($config){
		$this->conn = \Elasticsearch\ClientBuilder::create()->setHosts($config['connection_string'])->build();
	}
	
	public function insert($collection, $values){
		
		$params = [];
		$params['body']		= $values;
		$params['index']	= $this->index;
		$params['type'] 	= $collection;
		try {
			$result =  $this->conn->index($params);
			if($result['created']){
				return $result['_id'];
			}
			else{
				return null;
			}
		}
		catch (Exception $e) {
			return null;
		}
	}

	public function get($collection, $id){
		
		$params = [];
		$params['index']	= $this->index;
		$params['type'] 	= $collection;
		$params['id'] 		= $id;
		try{
			$result =  $this->conn->get($params);
			if($result['found']){
				$result['_source']['_id'] = $result['_id'];
				return $result['_source'];
			}
			else{
				return null;
			}
		}
		catch (\Exception $e){
			return null;
		}
	}

	public function update($collection, $filter, $values){

		$docs =  $this->find( $collection, $filter, ['_id']);
		if($docs['total']===0){
			return 0;
		}
		$params = [];
		$params['index']	= $this->index;
		$params['type'] 	= $collection;
		$modified_count = 0;
		foreach ($docs['data'] as $doc){
			$params['id']   		= $doc['_id'];
			$params['body']['doc']	= $values;
			try {
				$return = $this->conn->update($params);
				if($return['_shards']['successful']==1){
					$modified_count++;
				}
			} 
			catch (Exception $e) {
				// should we throw exception? Probably not.
			}
		}
		return $modified_count;
	}

	public function delete($collection, $filter){
		
		if(isset($filter['_id'])){
			$params = [];
			$params['index']	= $this->index;
			$params['type'] 	= $collection;
			$params['id']		= $filter['_id'];
			try{
				$result = $this->conn->delete($params);
			}
			catch (\Exception $e){
				$code = $e->getCode();
				if($code == 404){
					// @TODO: Should we throw exception for the attempts to delete not exists values?
				}
				return 0;
			}
			if($result['found']){
				return 1;
			}
		}
		else{
			$params = [];
			$params['index']	= $this->index;
			$params['type'] 	= $collection;
			$params['fields']	= "_id";

			$result =  $this->find("users", $filter,['_id'],null,0,1 );
			if($result['total'] == 1){
				$params = [];
				$params['index']	= $this->index;
				$params['type'] 	= $collection;
				$params['id']		= $result['data']['_id'];
				try{
					$result = $this->conn->delete($params);
				}
				catch (\Exception $e){
					$code = $e->getCode();
					if($code == 404){
						// @TODO: Should we throw exception for the attempts to delete not exists values?
					}
					return 0;
				}
				if($result['found']){
					return 1;
				}
			}
		}
		return 0;
	}

	public function find($collection, $filter, $fields=null, $sort=null, $start=0, $limit=25){

		$return_type 	= '_source';
		$params 		=[];
		$params['index']= $this->index;
		$params['type'] = $collection;
		$filters = ElasticSearch::build_filter($filter);
		$params['body'] = [
			'query'	=> [
				'filtered' => [
					'filter' =>	[
						'bool' => [
							'must' => [$filters]
						]
					]
				]
			]
		];
		$count =  $this->conn->count( $params );
		if($fields !== null){
			$params['fields'] = implode(",", $fields);
			$return_type = 'fields';
		}
		if($sort !== null){
			$params['sort']="";
			foreach ( $sort as $sort_key=> $sort_dir ){
				if($params['sort']!=""){
					$params['sort'] .=",";
				}
				$params['sort'] .= $sort_key . ":" . $sort_dir;
			}
		}
		if($fields != ""){
			$params['fields']	= $fields;
			$return_type		= 'fields';
		}
		$params['from']  	= (int) $start;
		$params['size']  	= (int) $limit;
		$return =  $this->conn->search( $params );
		if($return['hits']['total'] == 0){
			return ['total' => 0, 'data' => null];
		}
		if($limit == 1){
			$return['hits']['hits'][0][$return_type]['_id'] = $return['hits']['hits'][0]['_id'] ;
			return ['total' => 1, 'data' => $return['hits']['hits'][0][$return_type]];
		}
		else{
			$result = array();
			foreach ($return['hits']['hits'] as $item){
				$item[$return_type]['_id']= $item['_id'];
				$result[]=$item[$return_type];
			}
			return ['total' => $count['count'], 'data' => $result];
			
	
		}
	}
	
	public function query($query){
		// reserved		
	}

	private static function build_filter($filter){

		$filters = [];
		$prev_key='';
		foreach ($filter as $key=>$value){
			if(strpos($key,"__")!==false){
				preg_match('/__(.*?)$/i',$key, $matches );
				$operator = $matches[1];
				$key = str_replace($matches[0], "", $key);
				$filters[] = ['range' => [$key=>[$operator => $value]]];
			}
			else{
				$filters[] = ['term' => [$key => $value]];
				
			}
			$prev_key=$key;
		}
		return $filters;
		
	}
}