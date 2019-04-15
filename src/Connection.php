<?php

namespace Viloveul\MySql;

use PDO;
use PDOException;
use Viloveul\MySql\QueryBuilder;
use Viloveul\Database\QueryException;
use Viloveul\Database\ConnectionException;
use Viloveul\Database\Contracts\Model as IModel;
use Viloveul\Database\Contracts\Connection as IConnection;
use Viloveul\Database\Contracts\QueryBuilder as IQueryBuilder;

class Connection implements IConnection
{
    /**
     * @var mixed
     */
    protected $pdo = null;

    /**
     * @var mixed
     */
    protected $prefix;

    /**
     * @var mixed
     */
    private $dsn;

    /**
     * @var mixed
     */
    private $passwd;

    /**
     * @var mixed
     */
    private $user;

    /**
     * @param string $dsn
     * @param string $user
     * @param string $passwd
     * @param string $prefix
     */
    public function __construct(string $dsn, string $user, string $passwd, string $prefix = '')
    {
        $this->dsn = $dsn;
        $this->user = $user;
        $this->passwd = $passwd;
        $this->prefix = $prefix;
    }

    /**
     * @param string $query
     */
    public function compile(string $query): string
    {
        if (strpos($query, '{{') !== false && strpos($query, '}}') !== false) {
            return preg_replace('~{{\s+?([a-zA-Z0-9\_]+)\s+?}}~', "`{$this->prefix}\\1`", $query);
        } else {
            return $query;
        }
    }

    public function connect(): void
    {
        try {
            $this->pdo = new PDO('mysql:' . $this->dsn, $this->user, $this->passwd);
            $this->pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
        } catch (PDOException $e) {
            throw new ConnectionException($e->getMessage());
        }
    }

    public function disconnect(): void
    {
        if ($this->isConnected() === true) {
            $this->pdo = null;
        }
    }

    /**
     * @return mixed
     */
    public function isConnected(): bool
    {
        return $this->pdo !== null;
    }

    /**
     * @param IModel $model
     */
    public function newQuery(): IQueryBuilder
    {
        return new QueryBuilder($this);
    }

    /**
     * @param string $identifier
     */
    public function prep(string $identifier): string
    {
        if ($identifier === '*') {
            return $identifier;
        }
        return '`' . trim($identifier, '`"') . '`';
    }

    /**
     * @param  string  $query
     * @param  array   $params
     * @return mixed
     */
    public function runCommand(string $query, array $params = [])
    {
        try {
            $compiled = $this->compile($query);
            $query = $this->pdo->prepare($compiled);
            $query->execute($params);
            return $query;
        } catch (PDOException $e) {
            throw new QueryException($e->getMessage());
        }
    }
}
