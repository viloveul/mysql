<?php

namespace Viloveul\MySql;

use Viloveul\Database\Contracts\Schema as ISchema;
use Viloveul\Database\Contracts\Connection as IConnection;

class Schema implements ISchema
{
    /**
     * @var array
     */
    private $columnExists = [];

    /**
     * @var array
     */
    private $columns = [];

    /**
     * @var mixed
     */
    private $connection;

    /**
     * @var array
     */
    private $indexes = [];

    /**
     * @var mixed
     */
    private $isExists = false;

    /**
     * @var mixed
     */
    private $name;

    /**
     * @var array
     */
    private $options = [];

    /**
     * @var mixed
     */
    private $pointer = -1;

    /**
     * @var array
     */
    private $primaries = [];

    /**
     * @var array
     */
    private $uniques = [];

    /**
     * @param $v
     */
    function default($v): ISchema{
        $this->columns[$this->pointer]['default'] = in_array($v, ['NULL', 'CURRENT_TIMESTAMP']) ? $v : "'{$v}'";
        return $this;
    }

    /**
     * @param IConnection $connection
     * @param string      $name
     * @param array       $options
     */
    public function __construct(IConnection $connection, string $name, array $options = [])
    {
        $this->connection = $connection;
        $this->name = $name;
        $this->options = $options;
        $q = 'SELECT `COLUMN_NAME` FROM `information_schema`.`COLUMNS` WHERE `TABLE_NAME` = :tab AND `TABLE_SCHEMA` = :sch';
        $c = $connection->execute($q, [
            ':tab' => $connection->getPrefix() . $name,
            ':sch' => $connection->getDbName(),
        ]);
        while ($col = $c->fetchColumn()) {
            $this->columnExists[] = $col;
        }
        $q = 'SELECT COUNT(*) FROM `information_schema`.`TABLES` WHERE `TABLE_NAME` = :tab AND `TABLE_SCHEMA` = :sch';
        $c = $connection->execute($q, [
            ':tab' => $connection->getPrefix() . $name,
            ':sch' => $connection->getDbName(),
        ]);
        $this->isExists = $c->fetchColumn() > 0;
    }

    /**
     * @param string $name
     */
    public function hasColumn(string $name): bool
    {
        return in_array($name, $this->columnExists);
    }

    /**
     * @return mixed
     */
    public function increment(): ISchema
    {
        $this->columns[$this->pointer]['attr'] = $this->columns[$this->pointer]['attr'] . ' AUTO_INCREMENT';
        return $this;
    }

    /**
     * @param string $column
     */
    public function index(string $column = null): ISchema
    {
        if (empty($column)) {
            $this->indexes[$this->columns[$this->pointer]['name']] = $this->columns[$this->pointer]['name'];
        } else {
            $this->indexes[implode('_', func_get_args())] = func_get_args();
        }
        return $this;
    }

    /**
     * @return mixed
     */
    public function nullable(): ISchema
    {
        $this->columns[$this->pointer]['attr'] = str_replace('NOT NULL', 'NULL', $this->columns[$this->pointer]['attr']);
        return $this;
    }

    /**
     * @return mixed
     */
    public function primary(): ISchema
    {
        $this->primaries[] = $this->columns[$this->pointer]['name'];
        return $this;
    }

    public function run()
    {
        $values = [];
        if ($this->isExists === false) {
            foreach ($this->columns as $column) {
                $col = $this->connection->quote($column['name']) . ' ' . $column['type'] . ' ' . $column['attr'];
                if ($column['default'] !== null) {
                    $col .= ' DEFAULT ' . $column['default'];
                }
                $values[] = preg_replace('/\s+/', ' ', $col);
            }
            if ($this->primaries) {
                $values[] = 'PRIMARY KEY(' . implode(', ', array_map([$this->connection, 'quote'], $this->primaries)) . ')';
            }
            if ($this->uniques) {
                foreach ($this->uniques as $k => $unique) {
                    $unq = 'UNIQUE KEY ' . $this->connection->quote("{$this->name}_{$k}_unique");
                    if (is_scalar($unique)) {
                        $unq .= ' (' . $this->connection->quote($unique) . ')';
                    } else {
                        $unq .= ' (' . implode(', ', array_map([$this->connection, 'quote'], $unique)) . ')';
                    }
                    $values[] = $unq;
                }
            }
            if ($this->indexes) {
                foreach ($this->indexes as $k => $index) {
                    $key = 'KEY ' . $this->connection->quote("{$this->name}_{$k}_unique");
                    if (is_scalar($index)) {
                        $key .= ' (' . $this->connection->quote($index) . ')';
                    } else {
                        $key .= ' (' . implode(', ', array_map([$this->connection, 'quote'], $index)) . ')';
                    }
                    $values[] = $key;
                }
            }

            $q = 'CREATE TABLE {{' . $this->name . '}} (' . implode(',' . PHP_EOL, $values) . ') ENGINE=InnoDB;';
        } else {
            foreach ($this->columns as $column) {
                if (!$this->hasColumn($column['name'])) {
                    $col = 'ADD COLUMN ' . $this->connection->quote($column['name']) . ' ' . $column['type'] . ' ' . $column['attr'];
                    if ($column['default'] !== null) {
                        $col .= ' DEFAULT ' . $column['default'];
                    }
                    $values[] = preg_replace('/\s+/', ' ', $col);
                }
            }
            if ($this->uniques) {
                foreach ($this->uniques as $k => $unique) {
                    if ($this->checkComposite(is_scalar($unique) ? [$unique] : $unique) === false) {
                        $unq = 'ADD UNIQUE ' . $this->connection->quote("{$this->name}_{$k}_unique");
                        if (is_scalar($unique)) {
                            $unq .= ' (' . $this->connection->quote($unique) . ')';
                        } else {
                            $unq .= ' (' . implode(', ', array_map([$this->connection, 'quote'], $unique)) . ')';
                        }
                        $values[] = $unq;
                    }
                }
            }
            if ($this->indexes) {
                foreach ($this->indexes as $k => $index) {
                    if ($this->checkComposite(is_scalar($unique) ? [$unique] : $unique) === false) {
                        $key = 'ADD INDEX ' . $this->connection->quote("{$this->name}_{$k}_unique");
                        if (is_scalar($index)) {
                            $key .= ' (' . $this->connection->quote($index) . ')';
                        } else {
                            $key .= ' (' . implode(', ', array_map([$this->connection, 'quote'], $index)) . ')';
                        }
                        $values[] = $key;
                    }
                }
            }
            $q = 'ALTER TABLE {{' . $this->name . '}} ' . implode(', ', $values);
        }

        empty($values) or $this->connection->execute($q);
    }

    /**
     * @param string              $name
     * @param int                 $type
     * @param ISchemaTYPE_VARCHAR $value
     */
    public function set(string $name, int $type = ISchema::TYPE_VARCHAR, $value = null): ISchema
    {
        $column = [
            'name' => $name,
            'attr' => "NOT NULL",
            'default' => null,
        ];
        switch ($type) {
            case ISchema::TYPE_BIGINT:
                $column['type'] = 'bigint';
                break;
            case ISchema::TYPE_BINARY:
                $column['type'] = 'binary';
                break;
            case ISchema::TYPE_BLOB:
                $column['type'] = 'blob';
                break;
            case ISchema::TYPE_CHAR:
                $column['type'] = sprintf('char(%d)', $value ?: 255);
                break;
            case ISchema::TYPE_DATE:
                $column['type'] = 'date';
                break;
            case ISchema::TYPE_DATETIME:
                $column['type'] = 'datetime';
                break;
            case ISchema::TYPE_DECIMAL:
                $column['type'] = 'decimal';
                break;
            case ISchema::TYPE_ENUM:
                $values = array_map(function ($v) {
                    return "'{$v}'";
                }, $value);
                $column['type'] = sprintf('enum(%s)', implode(', ', $values));
                break;
            case ISchema::TYPE_INT:
                $column['type'] = 'int';
                break;
            case ISchema::TYPE_LONGBLOB:
                $column['type'] = 'longblob';
                break;
            case ISchema::TYPE_LONGTEXT:
                $column['type'] = 'longtext';
                break;
            case ISchema::TYPE_MEDBLOB:
                $column['type'] = 'mediumblob';
                break;
            case ISchema::TYPE_MEDINT:
                $column['type'] = 'mediumint';
                break;
            case ISchema::TYPE_MEDTEXT:
                $column['type'] = 'mediumtext';
                break;
            case ISchema::TYPE_SMALLBLOB:
                $column['type'] = 'smallblob';
                break;
            case ISchema::TYPE_SMALLINT:
                $column['type'] = 'smallint';
                break;
            case ISchema::TYPE_SMALLTEXT:
                $column['type'] = 'smalltext';
                break;
            case ISchema::TYPE_TEXT:
                $column['type'] = 'text';
                break;
            case ISchema::TYPE_TIME:
                $column['type'] = 'time';
                break;
            case ISchema::TYPE_TIMESTAMP:
                $column['type'] = 'timestamp';
                break;
            case ISchema::TYPE_VARCHAR:
                $column['type'] = sprintf('varchar(%d)', $value ?: 255);
                break;
            case ISchema::TYPE_YEAR:
                $column['type'] = 'year';
                break;
            default:
                $column['type'] = sprintf('varchar(%d)', $value ?: 255);
                break;
        }
        $this->pointer++;
        $this->columns[$this->pointer] = $column;
        return $this;
    }

    /**
     * @param  string  $column
     * @return mixed
     */
    public function unique(string $column = null): ISchema
    {
        if (empty($column)) {
            $this->uniques[$this->columns[$this->pointer]['name']] = $this->columns[$this->pointer]['name'];
        } else {
            $this->uniques[implode('_', func_get_args())] = func_get_args();
        }
        return $this;
    }

    /**
     * @return mixed
     */
    public function unsigned(): ISchema
    {
        $this->columns[$this->pointer]['attr'] = 'UNSIGNED ' . $this->columns[$this->pointer]['attr'];
        return $this;
    }

    /**
     * @param array $columns
     */
    protected function checkComposite(array $columns): bool
    {
        return array_intersect($columns, $this->columnExists) === $columns;
    }
}
