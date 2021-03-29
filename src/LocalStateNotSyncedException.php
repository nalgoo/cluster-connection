<?php

namespace Nalgoo\ClusterConnection;

use Doctrine\DBAL\Driver\AbstractDriverException;
use Doctrine\DBAL\Driver\DriverException;

class LocalStateNotSyncedException extends AbstractDriverException implements DriverException
{
    public static function withNode(string $failedNode): static
    {
        return new static(sprintf('Local state of node %s is not "Synced"', $failedNode));
    }
}
