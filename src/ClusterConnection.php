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
use Doctrine\DBAL\Exception\DriverException;
use Doctrine\DBAL\Exception\ForeignKeyConstraintViolationException;
use Doctrine\DBAL\Exception\InvalidFieldNameException;
use Doctrine\DBAL\Exception\NonUniqueFieldNameException;
use Doctrine\DBAL\Exception\NotNullConstraintViolationException;
use Doctrine\DBAL\Exception\SyntaxErrorException;
use Doctrine\DBAL\Exception\TableExistsException;
use Doctrine\DBAL\Exception\TableNotFoundException;
use Doctrine\DBAL\Exception\UniqueConstraintViolationException;

class ClusterConnection extends Connection
{
	const MAX_FAILED_ATTEMPTS = 2;

	const SELECTION_MODE_ROUND_ROBIN = 1;

    const SELECTION_MODE_PRIORITY = 2;

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

    /**
     * @var int
     */
	private $nodeSelectionMode = self::SELECTION_MODE_ROUND_ROBIN;

    /**
     * @var int
     */
	private $maxFailedAttempts = self::MAX_FAILED_ATTEMPTS;

	/**
	 * @var \Throwable|null
	 */
	private $lastError;

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

		$params = array_merge(
            ['url' => $url, 'wrapperClass' => self::class, 'driverClass' => ClusterAwarePDOMysqlDriver::class],
		    $additionalParams
        );

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

	public function setNodeSelectionMode(int $mode)
    {
        $this->nodeSelectionMode = $mode;
    }

    public function setMaxFailedAttempts(int $number)
    {
        $this->maxFailedAttempts = $number;
    }

	private function safeCall($function, $args)
	{
		try {
			return call_user_func_array(['parent', $function], $args);
		} catch (DriverException $e) {
			$this->markFailedAttempt();
			return $this->safeCall($function, $args);
		}
	}

	private function markFailedAttempt()
    {
        $this->failedAttempts[$this->selectedNode]++;

        $this->_conn = null;
        $this->selectedNode = null;
    }

    /**
	 * @return string
	 * @throws \Doctrine\DBAL\ConnectionException
	 */
	private function selectNode(): string
	{
	    $node = null;

	    switch ($this->nodeSelectionMode) {
            case self::SELECTION_MODE_ROUND_ROBIN:
                $node = $this->selectNodeRoundRobin();
                break;
            case self::SELECTION_MODE_PRIORITY:
                $node = $this->selectNodePriority();
                break;
        }

        if (!$node) {
        	$message = 'No available nodes left to connect to.';
        	if ($this->lastError) {
        		$message .= ' Last error: ' . $this->lastError->getMessage();
	        }
            throw new \Doctrine\DBAL\ConnectionException($message);
        }

        return $node;
	}

	private function selectNodePriority(): ?string
    {
        foreach ($this->nodes as $node) {
            if ($this->failedAttempts[$node] < $this->maxFailedAttempts) {
                return $node;
            }
        }

        return null;
    }

    private function selectNodeRoundRobin(): ?string
    {
        for ($failedAttempts = 0; $failedAttempts < $this->maxFailedAttempts; $failedAttempts++) {
            foreach ($this->nodes as $node) {
                if ($this->failedAttempts[$node] <= $failedAttempts) {
                    return $node;
                }
            }
        }

        return null;
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

    private function queryLocalState()
    {
        $statement = $this->query('SHOW GLOBAL STATUS LIKE "wsrep_local_state_comment"');

        $result = $statement->fetchColumn(1);

        if (!$result) {
            return true; // probably not in cluster at all
        }

        if ($result !== 'Synced') {
            throw LocalStateNotSyncedException::withNode($this->selectedNode);
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

            if (count($this->nodes) > 0) {
                $this->queryLocalState();
            }
        } catch (DriverException $e) {
        	if (
        		$e instanceof NotNullConstraintViolationException
		        || $e instanceof SyntaxErrorException
		        || $e instanceof NonUniqueFieldNameException
		        || $e instanceof InvalidFieldNameException
		        || $e instanceof UniqueConstraintViolationException
		        || $e instanceof ForeignKeyConstraintViolationException
		        || $e instanceof TableNotFoundException
		        || $e instanceof TableExistsException
	        ) {
        		// some exceptions should stop trying SQL query again
        		throw $e;
	        }

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
