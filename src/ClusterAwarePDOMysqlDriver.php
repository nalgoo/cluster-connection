<?php

namespace Nalgoo\ClusterConnection;

use Doctrine\DBAL\Driver\DriverException;
use Doctrine\DBAL\Driver\PDOMySql\Driver;

class ClusterAwarePDOMysqlDriver extends Driver
{
	public function convertException($message, DriverException $exception)
	{
		// 1047 - WSREP has not yet prepared node for application use
		if ($exception->getErrorCode() === 1047) {
			return new ClusterException($message, $exception);
		}

		return parent::convertException($message, $exception);
	}

}
