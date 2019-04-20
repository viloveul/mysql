<?php

namespace Viloveul\MySql;

use PDO;
use PDOException;
use Viloveul\MySql\Query;
use Viloveul\MySql\Schema;
use Viloveul\MySql\Compiler;
use Viloveul\MySql\Condition;
use Viloveul\Database\QueryException;
use Viloveul\Database\ConnectionException;
use Viloveul\Database\Contracts\Model as IModel;
use Viloveul\Database\Contracts\Query as IQuery;
use Viloveul\Database\Contracts\Schema as ISchema;
use Viloveul\Database\Contracts\Compiler as ICompiler;
use Viloveul\Database\Connection as AbstractConnection;
use Viloveul\Database\Contracts\Condition as ICondition;

class Connection extends AbstractConnection
{
    /**
     * @var mixed
     */
    protected $pdo = null;

    /**
     * @var array
     */
    private $options = [];

    /**
     * @var mixed
     */
    private $password;

    /**
     * @var mixed
     */
    private $username;

    /**
     * @param string $username
     * @param string $password
     * @param string $name
     * @param string $host
     * @param string $port
     * @param string $prefix
     * @param array  $options
     */
    public function __construct(
        string $username,
        string $password,
        string $name,
        string $host,
        string $port,
        string $prefix = '',
        array $options = []
    ) {
        $this->username = $username;
        $this->password = $password;
        $this->name = $name ?: 'viloveul';
        $this->host = $host ?: '127.0.0.1';
        $this->port = $port ?: 3306;
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
            $dsn = "mysql:dbname={$this->name};host={$this->host};port={$this->port}";
            $this->pdo = new PDO($dsn, $this->username, $this->password);
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
     * @param IQuery $builder
     */
    public function newCompiler(IQuery $builder): ICompiler
    {
        return new Compiler($this, $builder);
    }

    /**
     * @param ICompiler $compiler
     */
    public function newCondition(ICompiler $compiler): ICondition
    {
        return new Condition($compiler);
    }

    /**
     * @param IModel $model
     */
    public function newQuery(): IQuery
    {
        return new Query($this);
    }

    /**
     * @param string $name
     * @param array  $options
     */
    public function newSchema(string $name, array $options = []): ISchema
    {
        return new Schema($this, $name, $options);
    }

    /**
     * @param string $query
     */
    public function prepare(string $query): string
    {
        if (strpos($query, '{{') !== false && strpos($query, '}}') !== false) {
            return preg_replace('~{{\s?([a-zA-Z0-9\_]+)\s?}}~', "`{$this->prefix}\\1`", $query);
        } else {
            return $query;
        }
    }

    /**
     * @param  string  $identifier
     * @return mixed
     */
    public function quote(string $identifier): string
    {
        if ($identifier === '*') {
            return $identifier;
        }
        return '`' . trim($identifier, '`"') . '`';
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
    public function transaction(): bool
    {
        if (!$this->inTransaction()) {
            return $this->pdo->beginTransaction();
        }
        return true;
    }
}
