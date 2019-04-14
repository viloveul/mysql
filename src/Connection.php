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
    protected $pdo;

    /**
     * @var mixed
     */
    protected $prefix;

    /**
     * @param string $dsn
     * @param string $user
     * @param string $password
     * @param string $prefix
     */
    public function __construct(string $dsn, string $user, string $password, string $prefix = '')
    {
        try {
            $this->pdo = new PDO('mysql:' . $dsn, $user, $password);
            $this->prefix = $prefix;
        } catch (PDOException $e) {
            throw new ConnectionException($e->getMessage());
        }
    }

    /**
     * @param string $query
     */
    public function compile(string $query): string
    {
        if (strpos($query, '{{') !== false && strpos($query, '}}') !== false) {
            return preg_replace('~{{\s?([a-zA-Z0-9\_]+)\s?}}~', "{$this->prefix}\\1", $query);
        } else {
            return $query;
        }
    }

    /**
     * @param IModel $model
     */
    public function newQuery(): IQueryBuilder
    {
        return new QueryBuilder($this);
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
