<?php

namespace Viloveul\MySql;

use Closure;
use Exception;
use Viloveul\Database\Collection;
use Viloveul\Database\Expression;
use Viloveul\Database\Contracts\Model as IModel;
use Viloveul\Database\Contracts\Condition as ICondition;
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
    protected $groups = [];

    /**
     * @var array
     */
    protected $havingCondition;

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
    protected $whereCondition;

    /**
     * @var array
     */
    protected $withCounts = [];

    /**
     * @var mixed
     */
    private $compiler;

    /**
     * @param IConnection $connection
     */
    public function __construct(IConnection $connection)
    {
        $this->connection = $connection;
        $this->compiler = $connection->newCompiler($this);
        $this->whereCondition = $connection->newCondition($this->compiler);
        $this->havingCondition = $connection->newCondition($this->compiler);
    }

    public function __destruct()
    {
        $this->whereCondition->clear();
        $this->havingCondition->clear();
        $this->connection = null;
        $this->compiler = null;
        $this->whereCondition = null;
        $this->havingCondition = null;
    }

    /**
     * @param  $value
     * @return mixed
     */
    public function addParam($value): string
    {
        $key = ':bind_' . $this->getModel()->getAlias() . '_' . count($this->bindParams);
        $this->bindParams[$key] = $value;
        return $key;
    }

    /**
     * @return mixed
     */
    public function count(): int
    {
        $this->select('count(*)');
        $query = $this->connection->execute($this->getQuery(false), $this->getParams());
        return $query->fetchColumn();
    }

    public function delete()
    {
        $model = $this->getModel();
        $attributes = $model->oldAttributes();
        $primarys = (array) $model->primary();
        foreach ($primarys as $key) {
            if (array_key_exists($key, $attributes) && !$model->isAttributeCount($key)) {
                $this->where([$key => $attributes[$key]]);
            }
        }
        $alias = $this->compiler->quote($model->getAlias());
        $q = 'DELETE FROM ' . $alias . ' USING ' . $model->table() . ' AS ' . $alias;
        if ($where = $this->compiler->buildCondition($this->whereCondition->all())) {
            $q .= " WHERE {$where}";
        }

        try {
            $this->connection->execute($q, $this->getParams());
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
        $q = 'SELECT ' . $this->compiler->buildSelectedColumn($this->selectedColumns);
        $q .= ' FROM ' . $this->getModel()->table() . ' AS ' . $this->compiler->quote($this->getModel()->getAlias());
        if ($where = $this->compiler->buildCondition($this->whereCondition->all())) {
            $q .= " WHERE {$where}";
        }
        if ($groups = $this->compiler->buildGroupBy($this->groups)) {
            $q .= ' GROUP BY ' . $groups;
        }
        if ($having = $this->compiler->buildCondition($this->havingCondition->all())) {
            $q .= " HAVING {$having}";
        }
        if ($order = $this->compiler->buildOrderBy($this->orders)) {
            $q .= ' ORDER BY ' . $order;
        }
        if ($this->size > 0) {
            $q .= ' LIMIT ' . $this->size . ' OFFSET ' . abs($this->offset);
        }
        return $compile ? $this->connection->prepare($q) : $q;
    }

    /**
     * @return mixed
     */
    public function getResult()
    {
        $this->limit(1, 0);
        $this->getModel()->beforeFind();
        $query = $this->connection->execute($this->getQuery(false), $this->getParams());
        if ($result = $query->fetch()) {
            $model = clone $this->getModel();
            $model->setAttributes($result);
            foreach ($this->relations as $name => $callback) {
                if (is_callable($callback)) {
                    $model->load($name, $callback);
                } else {
                    $model->load($name);
                }
            }
            foreach ($this->withCounts as $name => $callback) {
                if ($rel = $this->compiler->parseRelations($name, $this->getModel()->relations())) {
                    [$type, $class, $through, $keys, $use] = $rel;
                    $child = new $class();
                    $child->where(function ($where) use ($keys, $result, &$maps) {
                        foreach ($keys as $pk => $fk) {
                            $where->add([$fk => $result[$pk]], IQueryBuilder::OPERATOR_IN);
                        }
                    });

                    is_callable($use) and $use($child);
                    is_callable($callback) and $callback($child);

                    foreach ($keys as $key) {
                        $child->groupBy($key);
                    }

                    $model->setAttributes([
                        "{$name}_count" => $child->count(),
                    ]);

                    $child->resetState();
                }
            }
            $model->afterFind();
            return $model;
        } else {
            return false;
        }
    }

    /**
     * @param  array   $conditions
     * @param  array   $attributes
     * @return mixed
     */
    public function getResultOrCreate(array $conditions, array $attributes = []): IModel
    {
        $this->whereCondition->clear();
        if ($model = $this->where($conditions)->getResult()) {
            return $model;
        } else {
            $model = $this->getModel()->newInstance();
            $model->setAttributes($attributes);
            $model->setAttributes($conditions);
            $model->save();
            return $model;
        }
    }

    /**
     * @return mixed
     */
    public function getResults(): ICollection
    {
        $maps = [];
        $relations = [];
        $this->getModel()->beforeFind();
        $query = $this->connection->execute(
            $this->getQuery(false),
            $this->getParams()
        );

        if (method_exists($query, 'fetchAll')) {
            $results = $query->fetchAll();
        } else {
            $results = [];
            while ($row = $query->fetch()) {
                $results[] = $row;
            }
        }
        foreach ($this->relations as $name => $relation) {
            if ($rel = $this->compiler->parseRelations($name, $this->getModel()->relations())) {
                [$type, $class, $through, $keys, $use] = $rel;
                $model = new $class();
                $model->where(function ($where) use ($keys, $results, &$maps) {
                    foreach ($keys as $pk => $fk) {
                        if (!array_key_exists($pk, $maps)) {
                            $maps[$pk] = array_map(function ($m) use ($pk) {
                                return $m[$pk];
                            }, $results);
                        }
                        $where->add([$fk => $maps[$pk]], IQueryBuilder::OPERATOR_IN);
                    }
                });

                is_callable($use) and $use($model);
                is_callable($relation) and $relation($model);

                $relations[$name] = [
                    'maps' => $keys,
                    'type' => $type,
                    'values' => $model->getResults(),
                ];
            }
        }

        $counts = [];
        foreach ($this->withCounts as $name => $relation) {
            if ($rel = $this->compiler->parseRelations($name, $this->getModel()->relations())) {
                [$type, $class, $through, $keys, $use] = $rel;
                $model = new $class();
                $model->select('count(*)');
                $model->where(function ($where) use ($keys, $results, &$maps) {
                    foreach ($keys as $pk => $fk) {
                        if (!array_key_exists($pk, $maps)) {
                            $maps[$pk] = array_map(function ($m) use ($pk) {
                                return $m[$pk];
                            }, $results);
                        }
                        $where->add([$fk => $maps[$pk]], IQueryBuilder::OPERATOR_IN);
                    }
                });

                is_callable($use) and $use($model);
                is_callable($relation) and $relation($model);

                foreach ($keys as $key) {
                    $model->select($key);
                    $model->groupBy($key);
                }
                $query = $model->connection()->execute($model->getQuery(false), $model->getParams());
                $resultCounts = [];
                while ($c = $query->fetch()) {
                    $k = '';
                    foreach ($keys as $key) {
                        $k .= $c[$key];
                    }
                    $resultCounts[] = [
                        'value' => $c['count'],
                        'identifier' => $k,
                        'keys' => array_keys($keys),
                    ];
                }
                $counts[$name] = $resultCounts;
            }
        }

        $finalResults = array_map(function ($result) use ($counts) {
            foreach ($counts as $name => $c) {
                foreach ($c as $row) {
                    $k = '';
                    foreach ($row['keys'] as $key) {
                        $k .= $result[$key];
                    }
                    if ($row['identifier'] === $k) {
                        $result[$name . '_count'] = $row['value'];
                    }
                }
            }
            return $result;
        }, $results);

        return new Collection($this->getModel(), $finalResults, $relations);
    }

    /**
     * @param  string  $column
     * @return mixed
     */
    public function groupBy(string $column): IQueryBuilder
    {
        $this->groups[] = $this->compiler->normalizeColumn($column);
        return $this;
    }

    /**
     * @param  $expression
     * @param  int           $operator
     * @param  int           $separator
     * @return mixed
     */
    public function having(
        $expression,
        int $operator = IQueryBuilder::OPERATOR_EQUAL,
        int $separator = IQueryBuilder::SEPARATOR_AND
    ): IQueryBuilder{
        $this->havingCondition->add($expression, $operator, $separator);
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
     * @param string  $name
     * @param Closure $callback
     */
    public function load(string $name, Closure $callback = null): void
    {
        if ($relations = $this->compiler->parseRelations($name, $this->getModel()->relations())) {
            [$type, $class, $through, $keys, $use] = $relations;
            $model = new $class();
            $model->where(function (ICondition $where) use ($keys) {
                foreach ($keys as $parent => $child) {
                    $where->add([$child => $this->getModel()->{$parent}]);
                }
            });

            is_callable($use) and $use($model);
            is_callable($callback) and $callback($model);

            $model->beforeFind();

            if ($type === IModel::HAS_MANY) {
                $this->getModel()->setAttributes([$name => $model->getResults()]);
            } else {
                $this->getModel()->setAttributes([$name => $model->getResult()]);
            }

            $model->afterFind();
        }
        $this->getModel()->resetState();
    }

    /**
     * @param string $column
     */
    public function max(string $column)
    {
        $this->select("max({$column})");
        $query = $this->connection->execute($this->getQuery(false), $this->getParams());
        return $query->fetchColumn();
    }

    /**
     * @param string $column
     */
    public function min(string $column)
    {
        $this->select("min({$column})");
        $query = $this->connection->execute($this->getQuery(false), $this->getParams());
        return $query->fetchColumn();
    }

    /**
     * @param  $expression
     * @param  int           $operator
     * @return mixed
     */
    public function orWhere(
        $expression,
        int $operator = IQueryBuilder::OPERATOR_EQUAL
    ): IQueryBuilder {
        return $this->where($expression, $operator, IQueryBuilder::SEPARATOR_OR);
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
     * @param  string  $column
     * @param  int     $sort
     * @return mixed
     */
    public function orderBy(string $column, int $sort = IQueryBuilder::ORDER_ASC): IQueryBuilder
    {
        $this->orders[] = [
            'column' => $this->compiler->normalizeColumn($column),
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
        $duplicates = [];
        foreach ($attributes as $key => $value) {
            if (!($value instanceof IModel) && !$model->isAttributeCount($key)) {
                $columns[] = $this->compiler->quote($key);
                $params = $this->compiler->makeParams([$value]);
                $values[] = $params[0];
            }
        }

        $model->beforeSave();

        if ($model->isNewRecord()) {
            $q = 'INSERT INTO ' . $model->table();
            $q .= '(' . implode(', ', $columns) . ')';
            $q .= ' VALUES(' . implode(', ', $values) . ')';
            $q .= ' ON DUPLICATE KEY UPDATE';
            foreach ($columns as $key => $column) {
                $duplicates[] = " {$column} = {$values[$key]}";
            }
            $q .= implode(', ', $duplicates);
        } else {
            $primarys = (array) $model->primary();
            $origins = $model->oldAttributes();
            foreach ($primarys as $col) {
                if (array_key_exists($col, $origins)) {
                    $this->where([$col => $origins[$col]]);
                }
            }
            $arguments = [];
            foreach ($columns as $key => $value) {
                $arguments[] = "{$value} = {$values[$key]}";
            }
            $q = 'UPDATE ' . $model->table() . ' AS ' . $this->compiler->quote($model->getAlias()) . ' SET ' . implode(', ', $arguments);
            if ($where = $this->compiler->buildCondition($this->whereCondition->all())) {
                $q .= " WHERE {$where}";
            }
        }

        try {
            $this->connection->execute($q, $this->getParams());
            $model->resetState();
            $model->clearAttributes();
            $model->setAttributes($attributes);
            $model->afterSave();
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
        $column = $this->compiler->normalizeColumn($column);
        $alias = $this->compiler->makeColumnAlias($alias ? $alias : $column);
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
     * @param  $expression
     * @param  int           $operator
     * @param  int           $separator
     * @return mixed
     */
    public function where(
        $expression,
        int $operator = IQueryBuilder::OPERATOR_EQUAL,
        int $separator = IQueryBuilder::SEPARATOR_AND
    ): IQueryBuilder{
        $this->whereCondition->add($expression, $operator, $separator);
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
        Closure $callback = null,
        int $separator = IQueryBuilder::SEPARATOR_AND
    ): IQueryBuilder {
        if ($relation = $this->compiler->parseRelations($name, $this->getModel()->relations())) {
            [$type, $class, $through, $keys, $use] = $relation;
            $model = new $class();
            $model->setAlias($name);
            $model->select(1);
            $model->where(function (ICondition $where) use ($name, $keys, $model) {
                foreach ($keys as $parent => $child) {
                    $columns = [
                        $this->compiler->quote($name) . '.' . $this->compiler->quote($child),
                        $this->compiler->quote($this->getModel()->getAlias()) . '.' . $this->compiler->quote($parent),
                    ];
                    $where->add(new Expression(implode('=', $columns)));
                }
            });

            is_callable($callback) and $callback($model);
            is_callable($use) and $use($model);
            $condition = [
                'separator' => $separator,
                'argument' => 'EXISTS (' . $model->getQuery(true) . ')',
            ];
            $this->whereCondition->push($condition);
            $this->bindParams = array_merge($this->getParams(), $model->getParams());
        }
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

    /**
     * @param  string  $name
     * @param  Closure $callback
     * @return mixed
     */
    public function withCount(string $name, Closure $callback = null): IQueryBuilder
    {
        $this->withCounts[$name] = $callback === null ? $name : $callback;
        return $this;
    }
}
