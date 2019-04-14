<?php

namespace Viloveul\MySql;

use PDO;
use Closure;
use Viloveul\MySql\Collection;
use Viloveul\Database\Contracts\Model as IModel;
use Viloveul\Database\Contracts\Collection as ICollection;
use Viloveul\Database\Contracts\Connection as IConnection;
use Viloveul\Database\Contracts\QueryBuilder as IQueryBuilder;

class QueryBuilder implements IQueryBuilder
{
    /**
     * @var array
     */
    protected $bindParams = [];

    /**
     * @var mixed
     */
    protected $connection;

    /**
     * @var array
     */
    protected $havingConditions = [];

    /**
     * @var mixed
     */
    protected $model;

    /**
     * @var mixed
     */
    protected $offset = 0;

    /**
     * @var mixed
     */
    protected $orders = [];

    /**
     * @var array
     */
    protected $relations = [];

    /**
     * @var array
     */
    protected $selectedColumns = [];

    /**
     * @var mixed
     */
    protected $size = 10;

    /**
     * @var mixed
     */
    protected $table;

    /**
     * @var array
     */
    protected $whereConditions = [];

    /**
     * @param IConnection $connection
     */
    public function __construct(IConnection $connection)
    {
        $this->connection = $connection;
    }

    public function count(): int
    {
        $q = "SELECT COUNT(*) FROM {$this->getModel()->table()} AS t";
        if ($where = $this->buildWhereCondition()) {
            $q .= " WHERE {$where}";
        }
        return $this->connection->runCommand($q, $this->bindParams)->fetchColumn();
    }

    /**
     * @return mixed
     */
    public function getModel(): IModel
    {
        return $this->model;
    }

    /**
     * @return mixed
     */
    public function getParams(): array
    {
        return $this->bindParams;
    }

    public function getQuery(): string
    {
        $q = 'SELECT ' . $this->buildSelectedColumn() . ' FROM ' . $this->getModel()->table() . ' AS t';
        if ($where = $this->buildWhereCondition()) {
            $q .= " WHERE {$where}";
        }
        $q .= ' LIMIT ' . $this->size . ' OFFSET ' . $this->offset;
        if ($order = $this->buildOrderBy()) {
            $q .= ' ORDER BY ' . $order;
        }
        return $this->connection->compile($q);
    }

    /**
     * @return mixed
     */
    public function getResult()
    {
        $result = $this->connection->runCommand($this->getQuery(), $this->getParams())->fetch(PDO::FETCH_ASSOC);
        $model = clone $this->getModel();
        $model->setAttributes($result);
        return $model;
    }

    /**
     * @return mixed
     */
    public function getResults(): ICollection
    {
        $results = $this->connection->runCommand($this->getQuery(), $this->getParams())->fetchAll(PDO::FETCH_ASSOC);
        $maps = array_map(function ($result) {
            $model = clone $this->getModel();
            $model->setAttributes($result);
            return $model;
        }, $results);
        return new Collection($maps);
    }

    /**
     * @param  int     $size
     * @param  int     $offset
     * @return mixed
     */
    public function limit(int $size, int $offset = 0): IQueryBuilder
    {
        $this->size = $size;
        $this->offset = $offset;
        return $this;
    }

    /**
     * @param string $name
     */
    public function loadRelation(string $name): void
    {

    }

    /**
     * @param string $column
     */
    public function max(string $column): int
    {
        $q = "SELECT MAX(`{$column}`) FROM {$this->getModel()->table()} AS t";
        if ($where = $this->buildWhereCondition()) {
            $q .= " WHERE {$where}";
        }
        return $this->connection->runCommand($q, $this->bindParams)->fetchColumn();
    }

    /**
     * @param string $column
     */
    public function min(string $column): int
    {
        $q = "SELECT MIN(`{$column}`) FROM {$this->getModel()->table()} AS t";
        if ($where = $this->buildWhereCondition()) {
            $q .= " WHERE {$where}";
        }
        return $this->connection->runCommand($q, $this->bindParams)->fetchColumn();
    }

    /**
     * @param  string   $column
     * @param  $value
     * @param  int      $operator
     * @param  int      $separator
     * @return mixed
     */
    public function orWhere(
        string $column,
        $value,
        int $operator = IQueryBuilder::OPERATOR_EQUAL,
        int $separator = IQueryBuilder::SEPARATOR_AND
    ): IQueryBuilder {
        return $this->where($column, $value, $operator, IQueryBuilder::SEPARATOR_OR);
    }

    /**
     * @param  Closure $callback
     * @return mixed
     */
    public function orWhereGroup(Closure $callback): IQueryBuilder
    {
        return $this->whereGroup($callback, IQueryBuilder::SEPARATOR_OR);
    }

    /**
     * @param  string  $column
     * @param  int     $sort
     * @return mixed
     */
    public function orderBy(string $column, int $sort = IQueryBuilder::ORDER_ASC): IQueryBuilder
    {
        $this->orders[] = [
            'column' => $column,
            'sort' => $sort,
        ];
        return $this;
    }

    /**
     * @param array $columns
     */
    public function select(array $columns = []): IQueryBuilder
    {
        foreach ($columns as $col) {
            $this->selectedColumns[] = $col;
        }
        return $this;
    }

    /**
     * @param IModel $model
     */
    public function setModel(IModel $model): void
    {
        $this->model = $model;
    }

    /**
     * @param string   $column
     * @param $value
     * @param int      $operator
     * @param int      $separator
     */
    public function where(
        string $column,
        $value,
        int $operator = IQueryBuilder::OPERATOR_EQUAL,
        int $separator = IQueryBuilder::SEPARATOR_AND
    ): IQueryBuilder{
        $condition = [
            'separator' => $separator,
        ];
        $column = strpos($column, '.') === false ? "t.{$column}" : $column;
        $params = $this->makeParams(is_scalar($value) ? [$value] : (array) $value);
        switch ($operator) {
            case IQueryBuilder::OPERATOR_RANGE:
            case IQueryBuilder::OPERATOR_BEETWEN:
                $first = array_shift($params);
                $last = isset($params[0]) ? $params[0] : $first;
                if ($operator === IQueryBuilder::OPERATOR_RANGE) {
                    $condition['argument'] = "({$column} >= {$first} AND {$column} <= $last)";
                } else {
                    $condition['argument'] = "({$column} BEETWEN {$first} AND $last)";
                }
                break;

            case IQueryBuilder::OPERATOR_LIKE:
                $condition['argument'] = "{$column} LIKE {$params[0]}";
                $this->bindParams[$params[0]] = "%{$value}%";
                break;
            case IQueryBuilder::OPERATOR_LLIKE:
                $condition['argument'] = "{$column} LIKE {$params[0]}";
                $this->bindParams[$params[0]] = "%{$value}";
                break;
            case IQueryBuilder::OPERATOR_RLIKE:
                $condition['argument'] = "{$column} LIKE {$params[0]}";
                $this->bindParams[$params[0]] = "{$value}%";
                break;

            case IQueryBuilder::OPERATOR_EQUAL:
                $condition['argument'] = "{$column} = {$params[0]}";
                break;
            case IQueryBuilder::OPERATOR_GT:
                $condition['argument'] = "{$column} > {$params[0]}";
                break;
            case IQueryBuilder::OPERATOR_LT:
                $condition['argument'] = "{$column} < {$params[0]}";
                break;
            case IQueryBuilder::OPERATOR_GTE:
                $condition['argument'] = "{$column} >= {$params[0]}";
                break;
            case IQueryBuilder::OPERATOR_LTE:
                $condition['argument'] = "{$column} <= {$params[0]}";
                break;

            case IQueryBuilder::OPERATOR_IN:
            case IQueryBuilder::OPERATOR_NOT_IN:
            default:
                $op = $operator === OPERATOR_NOT_IN ? 'NOT IN' : 'IN';
                $cond = implode(', ', $params);
                $condition['argument'] = "{$column} {$op} ({$cond})";
                break;
        }
        $this->whereConditions[] = $condition;
        return $this;
    }

    /**
     * @param Closure $callback
     * @param int     $separator
     */
    public function whereGroup(Closure $callback, int $separator = IQueryBuilder::SEPARATOR_AND): IQueryBuilder
    {
        $whereConditions = $this->whereConditions;
        $callback($this);
        array_push($whereConditions, [
            'separator' => $separator,
            'argument' => '(' . $this->buildWhereCondition() . ')',
        ]);
        $this->whereConditions = $whereConditions;
        return $this;
    }

    /**
     * @param  string  $name
     * @return mixed
     */
    public function with(string $name): IQueryBuilder
    {
        $this->relations[] = $name;
        return $this;
    }

    protected function buildOrderBy(): string
    {
        $orders = [];
        foreach ($this->orders as $order) {
            $orders[] = $order['column'] . ' ' . ($order['sort'] === IQueryBuilder::ORDER_ASC ? 'ASC' : 'DESC');
        }
        return implode(', ', $orders);
    }

    protected function buildSelectedColumn(): string
    {
        $selects = [];
        if (empty($this->selectedColumns)) {
            return '*';
        }
        if (!in_array('*', $this->selectedColumns)) {
            foreach ($this->selectedColumns as $select) {
                $this->selects[] = $select;
            }
        }
        return implode(', ', array_unique($selects));
    }

    /**
     * @return mixed
     */
    protected function buildWhereCondition(): string
    {
        $conditions = '';
        foreach ($this->whereConditions as $condition) {
            if (!empty($conditions)) {
                $conditions .= ' ' . ($condition['separator'] === IQueryBuilder::SEPARATOR_AND ? 'AND' : 'OR') . ' ';
            }
            $conditions .= $condition['argument'];
        }
        return $conditions;
    }

    /**
     * @param  array   $params
     * @return mixed
     */
    protected function makeParams(array $params): array
    {
        $binds = [];
        foreach ($params as $value) {
            $k = count($this->bindParams);
            $this->bindParams[':bind_' . $k] = $value;
            $binds[] = ':bind_' . $k;
        }
        return $binds;
    }
}
