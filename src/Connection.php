<?php

namespace Viloveul\MySql;

use PDO;
use PDOException;
use Viloveul\MySql\Compiler;
use Viloveul\MySql\Condition;
use Viloveul\MySql\QueryBuilder;
use Viloveul\Database\QueryException;
use Viloveul\Database\ConnectionException;
use Viloveul\Database\Contracts\Model as IModel;
use Viloveul\Database\Contracts\Compiler as ICompiler;
use Viloveul\Database\Contracts\Condition as ICondition;
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
     * @var array
     */
    private $logs = [];

    /**
     * @var array
     */
    private $options = [];

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
     * @param array  $options
     */
    public function __construct(string $dsn, string $user, string $passwd, string $prefix = '', array $options = [])
    {
        $this->dsn = $dsn;
        $this->user = $user;
        $this->passwd = $passwd;
        $this->prefix = $prefix;
        $this->options = $options;
    }

    public function __destruct()
    {
        $this->disconnect();
        $this->logs = [];
    }

    /**
     * @return mixed
     */
    public function commit(): bool
    {
        if ($this->inTransaction()) {
            return $this->pdo->commit();
        }
        return false;
    }

    public function connect(): void
    {
        try {
            $this->pdo = new PDO('mysql:' . $this->dsn, $this->user, $this->passwd);
            $this->pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
            $this->pdo->setAttribute(PDO::ATTR_DEFAULT_FETCH_MODE, PDO::FETCH_ASSOC);
            foreach ($this->options as $key => $value) {
                $this->pdo->setAttribute($key, $value);
            }
        } catch (PDOException $e) {
            throw new ConnectionException($e->getMessage());
        }
    }

    public function disconnect(): void
    {
        if ($this->isConnected() === true) {
            if ($this->inTransaction()) {
                $this->rollback();
            }
            $this->pdo = null;
        }
    }

    /**
     * @param  string  $query
     * @param  array   $params
     * @return mixed
     */
    public function execute(string $query, array $params = [])
    {
        try {
            $compiled = $this->prepare($query);
            $query = $this->pdo->prepare($compiled);
            $query->execute($params);
            $this->logs[] = [
                'query' => $compiled,
                'params' => $params,
            ];
            return $query;
        } catch (PDOException $e) {
            throw new QueryException($e->getMessage());
        }
    }

    /**
     * @return mixed
     */
    public function inTransaction(): bool
    {
        return $this->pdo->inTransaction();
    }

    /**
     * @return mixed
     */
    public function isConnected(): bool
    {
        return $this->pdo !== null;
    }

    /**
     * @param IQueryBuilder $builder
     */
    public function newCompiler(IQueryBuilder $builder): ICompiler
    {
        return new Compiler($builder);
    }

    /**
     * @param IQueryBuilder $builder
     * @param ICompiler     $compiler
     */
    public function newCondition(IQueryBuilder $builder, ICompiler $compiler): ICondition
    {
        return new Condition($builder, $compiler);
    }

    /**
     * @param IModel $model
     */
    public function newQueryBuilder(): IQueryBuilder
    {
        return new QueryBuilder($this);
    }

    /**
     * @param string $query
     */
    public function prepare(string $query): string
    {
        if (strpos($query, '{{') !== false && strpos($query, '}}') !== false) {
            return preg_replace('~{{\s+?([a-zA-Z0-9\_]+)\s+?}}~', "`{$this->prefix}\\1`", $query);
        } else {
            return $query;
        }
    }

    /**
     * @return mixed
     */
    public function rollback(): bool
    {
        if ($this->inTransaction()) {
            return $this->pdo->rollback();
        }
        return false;
    }

    /**
     * @return mixed
     */
    public function showLogQueries(): array
    {
        return $this->logs;
    }

    /**
     * @return mixed
     */
    public function transaction(): bool
    {
        if (!$this->inTransaction()) {
            return $this->pdo->beginTransaction();
        }
        return true;
    }
}
