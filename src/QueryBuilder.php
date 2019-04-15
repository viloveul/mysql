<?php

namespace Viloveul\MySql;

use PDO;
use Closure;
use Exception;
use Viloveul\MySql\Collection;
use Viloveul\Database\Expression;
use Viloveul\Database\Contracts\Model as IModel;
use Viloveul\Database\Contracts\Collection as ICollection;
use Viloveul\Database\Contracts\Connection as IConnection;
use Viloveul\Database\Contracts\Expression as IExpression;
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
    protected $groups = [];

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
    protected $size = 0;

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

    /**
     * @return mixed
     */
    public function count(): int
    {
        $this->select('count(*)');
        $query = $this->connection->runCommand($this->getQuery(false), $this->getParams());
        return $query->fetchColumn();
    }

    public function delete()
    {
        $model = $this->getModel();
        $attributes = $model->oldAttributes();
        $primarys = (array) $model->primary();
        foreach ($primarys as $key) {
            if (array_key_exists($key, $attributes)) {
                $this->where($key, $attributes[$key]);
            }
        }
        $alias = $this->connection->prep($model->getAlias());
        $q = 'DELETE FROM ' . $alias . ' USING ' . $model->table() . ' AS ' . $alias;
        if ($where = $this->buildWhereCondition()) {
            $q .= " WHERE {$where}";
        }

        try {
            $this->connection->runCommand($q, $this->getParams());
            return true;
        } catch (Exception $e) {
            throw $e;
        }
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

    /**
     * @param bool $compile
     */
    public function getQuery(bool $compile = true): string
    {
        $q = 'SELECT ' . $this->buildSelectedColumn() . ' FROM ' . $this->getModel()->table() . ' AS ' . $this->connection->prep($this->getModel()->getAlias());
        if ($where = $this->buildWhereCondition()) {
            $q .= " WHERE {$where}";
        }
        if ($groups = $this->buildGroupBy()) {
            $q .= ' GROUP BY ' . $groups;
        }
        if ($order = $this->buildOrderBy()) {
            $q .= ' ORDER BY ' . $order;
        }
        if ($this->size > 0) {
            $q .= ' LIMIT ' . $this->size . ' OFFSET ' . abs($this->offset);
        }
        return $compile ? $this->connection->compile($q) : $q;
    }

    /**
     * @return mixed
     */
    public function getResult()
    {
        $this->limit(1, 0);
        $query = $this->connection->runCommand($this->getQuery(false), $this->getParams());
        if ($result = $query->fetch(PDO::FETCH_ASSOC)) {
            $model = clone $this->getModel();
            $model->setAttributes($result);
            return $model;
        } else {
            return false;
        }
    }

    /**
     * @return mixed
     */
    public function getResults(): ICollection
    {
        $query = $this->connection->runCommand($this->getQuery(false), $this->getParams());
        $results = $query->fetchAll(PDO::FETCH_ASSOC);
        $relations = [];
        if ($this->relations) {
            foreach ($this->relations as $name => $relation) {
                [$type, $class, $pk, $fk] = $this->getModel()->relations()[$name];
                $keys = array_map(function ($result) use ($pk) {
                    return $result[$pk];
                }, $results);
                $model = $class::where($fk, $keys, IQueryBuilder::OPERATOR_IN);
                is_callable($relation) and $relation($model);
                $childs = $model->getResults();
                foreach ($childs as $child) {
                    $relations[$name]['pk'] = $pk;
                    $relations[$name]['identifier'] = $child[$fk];
                    if ($type === IModel::HAS_MANY) {
                        $relations[$name]['values'][] = $child;
                    } else {
                        $relations[$name]['values'] = $child;
                    }
                }
            }
        }
        return new Collection($this->getModel(), $results, $relations);
    }

    /**
     * @param  string  $column
     * @return mixed
     */
    public function groupBy(string $column): IQueryBuilder
    {
        $this->groups[] = $column;
        return $this;
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
     * @param string $column
     */
    public function max(string $column)
    {
        $this->select("max({$column})");
        $query = $this->connection->runCommand($this->getQuery(false), $this->getParams());
        return $query->fetchColumn();
    }

    /**
     * @param string $column
     */
    public function min(string $column)
    {
        $this->select("min({$column})");
        $query = $this->connection->runCommand($this->getQuery(false), $this->getParams());
        return $query->fetchColumn();
    }

    /**
     * @param  string   $column
     * @param  $value
     * @param  int      $operator
     * @return mixed
     */
    public function orWhere(
        string $column,
        $value,
        int $operator = IQueryBuilder::OPERATOR_EQUAL
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
     * @param  string  $relation
     * @param  Closure $callback
     * @return mixed
     */
    public function orWhereHas(string $relation, Closure $callback): IQueryBuilder
    {
        return $this->whereHas($relation, $callback, IQueryBuilder::SEPARATOR_OR);
    }

    /**
     * @param  IExpression $expression
     * @return mixed
     */
    public function orWhereRaw(IExpression $expression): IQueryBuilder
    {
        return $this->whereRaw($expression, IQueryBuilder::SEPARATOR_OR);
    }

    /**
     * @param  string  $column
     * @param  int     $sort
     * @return mixed
     */
    public function orderBy(string $column, int $sort = IQueryBuilder::ORDER_ASC): IQueryBuilder
    {
        $this->orders[] = [
            'column' => $this->normalizeColumn($column),
            'sort' => $sort,
        ];
        return $this;
    }

    /**
     * @return mixed
     */
    public function save()
    {
        $model = $this->getModel();
        $attributes = $model->getAttributes();
        $columns = [];
        $values = [];
        foreach ($attributes as $key => $value) {
            if (!($value instanceof IModel)) {
                $columns[] = $this->connection->prep($key);
                $params = $this->makeParams([$value]);
                $values[] = $params[0];
            }
        }

        if ($model->isNewRecord()) {
            $q = 'INSERT INTO ' . $model->table() . ' AS ' . $this->connection->prep($model->getAlias());
            $q .= '(' . implode(', ', $columns) . ')';
            $q .= 'VALUES(' . implode(', ', $values) . ')';
        } else {
            $primarys = (array) $model->primary();
            $origins = $model->oldAttributes();
            foreach ($primarys as $col) {
                if (array_key_exists($col, $origins)) {
                    $this->where($col, $origins[$col]);
                }
            }
            $arguments = [];
            foreach ($columns as $key => $value) {
                $arguments[] = "{$value} = {$values[$key]}";
            }
            $q = 'UPDATE ' . $model->table() . ' AS ' . $this->connection->prep($model->getAlias()) . ' SET ' . implode(', ', $arguments);
            if ($where = $this->buildWhereCondition()) {
                $q .= " WHERE {$where}";
            }
        }

        try {
            $this->connection->runCommand($q, $this->getParams());
            $model->resetState();
            $model->setAttributes($attributes);
            return $model;
        } catch (Exception $e) {
            throw $e;
        }
    }

    /**
     * @param  string  $column
     * @param  string  $alias
     * @return mixed
     */
    public function select(string $column, string $alias = null): IQueryBuilder
    {
        $column = $this->normalizeColumn($column);
        $alias = $this->makeColumnAlias($alias ? $alias : $column);
        $this->selectedColumns[$alias] = $column;
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
        $column = $this->normalizeColumn($column);
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
                $op = $operator === IQueryBuilder::OPERATOR_NOT_IN ? 'NOT IN' : 'IN';
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
        if ($compiled = $this->buildWhereCondition()) {
            array_push($whereConditions, [
                'separator' => $separator,
                'argument' => '(' . $compiled . ')',
            ]);
        }
        $this->whereConditions = $whereConditions;
        return $this;
    }

    /**
     * @param  string  $name
     * @param  Closure $callback
     * @param  int     $separator
     * @return mixed
     */
    public function whereHas(
        string $name,
        Closure $callback,
        int $separator = IQueryBuilder::SEPARATOR_AND
    ): IQueryBuilder {
        [$type, $class, $pk, $fk] = $this->getModel()->relations()[$name];
        $model = new $class();
        $model->setAlias($name);
        $model->whereGroup($callback);
        $model->select(1);
        $columns = [
            $model->connection()->prep($name) . '.' . $model->connection()->prep($fk),
            $this->connection->prep($this->getModel()->getAlias()) . '.' . $this->connection->prep($pk),
        ];
        $model->whereRaw(new Expression(implode('=', $columns)));
        $this->whereConditions[] = [
            'separator' => $separator,
            'argument' => 'EXISTS (' . $model->getQuery(true) . ')',
        ];
        $this->bindParams = array_merge($this->getParams(), $model->getParams());
        return $this;
    }

    /**
     * @param  IExpression $expression
     * @param  int         $separator
     * @return mixed
     */
    public function whereRaw(
        IExpression $expression,
        int $separator = IQueryBuilder::SEPARATOR_AND
    ): IQueryBuilder{
        $this->whereConditions[] = [
            'separator' => $separator,
            'argument' => $expression->getCompiled(),
        ];
        return $this;
    }

    /**
     * @param  string  $name
     * @param  Closure $callback
     * @return mixed
     */
    public function with(string $name, Closure $callback = null): IQueryBuilder
    {
        $this->relations[$name] = $callback === null ? $name : $callback;
        return $this;
    }

    protected function buildGroupBy(): string
    {
        $groups = [];
        foreach ($this->groups as $col) {
            $groups[] = $this->normalizeColumn($col);
        }
        return implode(', ', $groups);
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
            foreach ($this->selectedColumns as $alias => $select) {
                $selects[] = ($alias == $select) ? $select : "{$select} AS {$alias}";
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
     * @param  string  $column
     * @return mixed
     */
    protected function makeColumnAlias(string $column): string
    {
        if (is_numeric($column)) {
            return $column;
        } else {
            preg_match_all('~\`([a-zA-Z0-9\_]+)\`~mi', $column, $matches);
            if (array_key_exists(1, $matches) && count($matches[1]) > 0) {
                return $this->connection->prep(end($matches[1]));
            } else {
                return $this->connection->prep(preg_replace('/[^a-zA-Z0-9\_\.]+/', '', $column));
            }
        }
    }

    /**
     * @param  array   $params
     * @return mixed
     */
    protected function makeParams(array $params): array
    {
        $binds = [];
        foreach ($params as $value) {
            $k = ':bind_' . $this->getModel()->getAlias() . '_' . count($this->bindParams);
            $this->bindParams[$k] = $value;
            $binds[] = $k;
        }
        return $binds;
    }

    /**
     * @param  string  $column
     * @return mixed
     */
    protected function normalizeColumn(string $column): string
    {
        if (is_numeric($column)) {
            return $column;
        } elseif (strpos($column, '(') !== false && strpos($column, ')') !== false) {
            return preg_replace_callback('#(\w+)\(([a-zA-Z0-9\_\.\`\"]+)\)#', function ($match) {
                $exploded = explode('.', $match[2]);
                if (count($exploded) === 1) {
                    array_unshift($exploded, $this->getModel()->getAlias());
                }
                $parts = array_map([$this->connection, 'prep'], $exploded);
                return $match[1] . '(' . implode('.', $parts) . ')';
            }, $column);
        } else {
            $exploded = explode('.', $column);
            if (count($exploded) === 1) {
                array_unshift($exploded, $this->getModel()->getAlias());
            }
            $parts = array_map([$this->connection, 'prep'], $exploded);
            return implode('.', $parts);
        }
    }
}
