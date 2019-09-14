<?php

namespace Nalgoo\ClusterConnection;

use Doctrine\Common\EventManager;
use Doctrine\DBAL\Cache\QueryCacheProfile;
use Doctrine\DBAL\Configuration;
use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Driver;
use Doctrine\DBAL\DriverManager;
use Doctrine\DBAL\Events;
use Doctrine\DBAL\Event;
use Doctrine\DBAL\Exception\ConnectionException;

class ClusterConnection extends Connection
{
	const MAX_FAILED_ATTEMPTS = 2;

	/**
	 * @var array
	 */
	private $nodes = [];

	/**
	 * @var array
	 */
	private $failedAttempts = [];

	/**
	 * @var array
	 */
	private $params = [];

	/**
	 * @var string|null
	 */
	private $selectedNode;

	public function __construct($params, Driver $driver, ?Configuration $config = null, ?EventManager $eventManager = null)
	{
		if (isset($params['pdo'])) {
			throw new \InvalidArgumentException('PDO should not be set when using ClusterConnection');
		}

		if (isset($params['host'])) {
			$host = $params['host'];
			if (isset($params['port'])) {
				$host .= ':' . $params['port'];
			}

			$this->addNode($host);
		}

		$this->params = $params;

		parent::__construct($params, $driver, $config, $eventManager);
	}

	/**
	 * @param string $url
	 * @param array $additionalParams
	 * @param Configuration|null $config
	 * @param EventManager|null $eventManager
	 * @return ClusterConnection
	 * @throws \Doctrine\DBAL\DBALException
	 */
	public static function createFromUrl(string $url, array $additionalParams = [], ?Configuration $config = null, ?EventManager $eventManager = null): self
	{
		$additionalHosts = [];

		$url = preg_replace_callback('/\,([^,\/]+)/', function($matches) use (&$additionalHosts) {
			$additionalHosts[] = $matches[1];
			return '';
		}, $url);

		$params = array_merge($additionalParams, ['url' => $url, 'wrapperClass' => self::class]);

		/** @var self $connection */
		$connection = DriverManager::getConnection($params, $config, $eventManager);

		foreach($additionalHosts as $host) {
			$connection->addNode($host);
		}

		return $connection;
	}

	public function addNode(string $node)
	{
		$this->nodes[] = $node;
		$this->failedAttempts[$node] = 0;
	}

	private function safeCall($function, $args)
	{
		try {
			return call_user_func_array(['parent', $function], $args);

		} catch (ConnectionException $e) {
			$this->failedAttempts[$this->selectedNode]++;
			$this->_conn = null;

			return $this->safeCall($function, $args);

		} catch (ClusterException $e) {
			$this->failedAttempts[$this->selectedNode]++;
			$this->_conn = null;

			return $this->safeCall($function, $args);
		}
	}

	### overloaded Connection methods

	/**
	 * @return bool
	 * @throws \Doctrine\DBAL\ConnectionException
	 */
	public function connect()
	{
		if ($this->_conn) {
			return false;
		}

		$this->selectedNode = $this->selectNode();

		try {
			$this->_conn = $this->connectTo($this->selectedNode);

		} catch (ConnectionException $e) {
			$this->failedAttempts[$this->selectedNode]++;
			$this->_conn = null;

			return $this->connect();
		}

		if ($this->_eventManager->hasListeners(Events::postConnect)) {
			$eventArgs = new Event\ConnectionEventArgs($this);
			$this->_eventManager->dispatchEvent(Events::postConnect, $eventArgs);
		}

		return true;
	}

	/**
	 * @return string
	 * @throws \Doctrine\DBAL\ConnectionException
	 */
	private function selectNode(): string
	{
		foreach ($this->nodes as $node) {
			if ($this->failedAttempts[$node] < self::MAX_FAILED_ATTEMPTS) {
				return $node;
			}
		}

		throw new \Doctrine\DBAL\ConnectionException('No available nodes left to connect to');
	}

	private function connectTo(string $node)
	{
		if (!preg_match('/^(.*)(:([0-9]+))?$/', $node, $matches)) {
			throw new \Doctrine\DBAL\ConnectionException('Cannot parse host name');
		}

		$params = $this->params;
		$params['host'] = $matches[1];

		if (isset($matches[3])) {
			$params['port'] = $matches[3];
		} else {
			unset($params['port']);
		}

		$driverOptions = $this->params['driverOptions'] ?? [];
		$user          = $this->params['user'] ?? null;
		$password      = $this->params['password'] ?? null;

		return $this->_driver->connect($params, $user, $password, $driverOptions);
	}

	public function getDatabasePlatform()
	{
		return $this->safeCall(__FUNCTION__, func_get_args());
	}

	public function quote($input, $type = null)
	{
		return $this->safeCall(__FUNCTION__, func_get_args());
	}

	public function executeQuery($query, array $params = [], $types = [], ?QueryCacheProfile $qcp = null)
	{
		return $this->safeCall(__FUNCTION__, func_get_args());
	}

	public function query()
	{
		return $this->safeCall(__FUNCTION__, func_get_args());
	}

	public function executeUpdate($query, array $params = [], array $types = [])
	{
		return $this->safeCall(__FUNCTION__, func_get_args());
	}

	public function exec($statement)
	{
		return $this->safeCall(__FUNCTION__, func_get_args());
	}

	public function errorCode()
	{
		return $this->safeCall(__FUNCTION__, func_get_args());
	}

	public function errorInfo()
	{
		return $this->safeCall(__FUNCTION__, func_get_args());
	}

	public function lastInsertId($seqName = null)
	{
		return $this->safeCall(__FUNCTION__, func_get_args());
	}

	public function beginTransaction()
	{
		return $this->safeCall(__FUNCTION__, func_get_args());
	}

	public function commit()
	{
		return $this->safeCall(__FUNCTION__, func_get_args());
	}

	public function rollBack()
	{
		return $this->safeCall(__FUNCTION__, func_get_args());
	}

	public function getWrappedConnection()
	{
		return $this->safeCall(__FUNCTION__, func_get_args());
	}

	public function ping()
	{
		return $this->safeCall(__FUNCTION__, func_get_args());
	}

}
