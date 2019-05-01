<?php

namespace Viloveul\MySql;

use PDO;
use PDOException;
use Viloveul\MySql\Query;
use Viloveul\MySql\Result;
use Viloveul\MySql\Schema;
use Viloveul\Database\QueryException;
use Viloveul\Database\ConnectionException;
use Viloveul\Database\Contracts\Model as IModel;
use Viloveul\Database\Contracts\Query as IQuery;
use Viloveul\Database\Contracts\Result as IResult;
use Viloveul\Database\Contracts\Schema as ISchema;
use Viloveul\Database\Connection as AbstractConnection;

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
            $this->pdo->setAttribute(PDO::ATTR_STRINGIFY_FETCHES, false);
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
    public function execute(string $query, array $params = []): IResult
    {
        try {
            $compiled = $this->prepare($query);
            $this->addLogQuery($compiled, $params);
            $query = $this->pdo->prepare($compiled);
            $query->execute($params);
            return new Result($query);
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
     * @param  string  $name
     * @return mixed
     */
    public function lastInsertedValue(string $name = null)
    {
        return $this->pdo->lastInsertId($name);
    }

    /**
     * @param  string  $column
     * @return mixed
     */
    public function makeAliasColumn(string $column, string $append = null): string
    {
        if (is_numeric($column)) {
            return $column;
        } else {
            preg_match_all('~\`([a-zA-Z0-9\_]+)\`~mi', $column, $matches);
            if (array_key_exists(1, $matches) && count($matches[1]) > 0) {
                $new = end($matches[1]);
            } else {
                $new = preg_replace('/[^a-zA-Z0-9\_\.]+/', '', $column);
            }
            if (!empty($append)) {
                // normalize
                $new = preg_replace('/\_id$/', '', trim($new, '`"'));
                if (stripos($new, 'id_') === 0) {
                    $new = substr($new, 3);
                }
                $new = $append . '_' . $new;
            }
            return $this->quote($new);
        }
    }

    /**
     * @param  string  $column
     * @param  string  $table
     * @return mixed
     */
    public function makeNormalizeColumn(string $column, string $table = null): string
    {
        if (is_numeric($column)) {
            return $column;
        } elseif (strpos($column, '(') !== false && strpos($column, ')') !== false) {
            return preg_replace_callback('#(\w+)\(([a-zA-Z0-9\_\.\`\"\*]+)\)#', function ($match) use ($table) {
                $exploded = explode('.', $match[2]);
                if (count($exploded) === 1 && strpos($match[2], '*') === false && !empty($table)) {
                    array_unshift($exploded, $table);
                }
                $parts = array_map([$this, 'quote'], $exploded);
                return $match[1] . '(' . implode('.', $parts) . ')';
            }, $column);
        } else {
            $exploded = explode('.', preg_replace('/[^a-zA-Z0-9\.\_\*]+/', '', $column));
            if (count($exploded) === 1 && !empty($table)) {
                array_unshift($exploded, $table);
            }
            $parts = array_map([$this, 'quote'], $exploded);
            return implode('.', $parts);
        }
    }

    /**
     * @param IModel $model
     */
    public function newQuery(): IQuery
    {
        $query = new Query();
        $query->setConnection($this);
        $query->initialize();
        return $query;
    }

    /**
     * @param string $name
     * @param array  $options
     */
    public function newSchema(string $name, array $options = []): ISchema
    {
        $schema = new Schema($name, $options);
        $schema->setConnection($this);
        $schema->initialize();
        return $schema;
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
