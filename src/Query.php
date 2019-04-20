<?php

namespace Viloveul\MySql;

use Closure;
use Exception;
use Viloveul\Database\Collection;
use Viloveul\Database\Expression;
use Viloveul\Database\Query as AbstractQuery;
use Viloveul\Database\Contracts\Model as IModel;
use Viloveul\Database\Contracts\Query as IQuery;
use Viloveul\Database\Contracts\Condition as ICondition;
use Viloveul\Database\Contracts\Collection as ICollection;

class Query extends AbstractQuery
{
    /**
     * @var array
     */
    protected $bindParams = [];

    /**
     * @var array
     */
    protected $groups = [];

    /**
     * @var array
     */
    protected $joins = [];

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
    protected $selectedColumns = [];

    /**
     * @var mixed
     */
    protected $size = 0;

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
        $alias = $this->connection->quote($model->getAlias());
        $q = 'DELETE FROM ' . $alias . ' USING ' . $model->table() . ' AS ' . $alias;
        if ($where = $this->getCompiler()->buildCondition($this->whereCondition->all())) {
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
     * @param bool $compile
     */
    public function getQuery(bool $compile = true): string
    {
        $q = 'SELECT ' . $this->getCompiler()->buildSelectedColumn($this->selectedColumns);
        $q .= ' FROM ' . $this->getModel()->table() . ' AS ' . $this->connection->quote($this->getModel()->getAlias());

        if (count($this->joins) > 0) {
            $q .= ' ' . implode(' ', $this->joins);
        }

        if ($where = $this->getCompiler()->buildCondition($this->whereCondition->all())) {
            $q .= " WHERE {$where}";
        }
        if ($groups = $this->getCompiler()->buildGroupBy($this->groups)) {
            $q .= ' GROUP BY ' . $groups;
        }
        if ($having = $this->getCompiler()->buildCondition($this->havingCondition->all())) {
            $q .= " HAVING {$having}";
        }
        if ($order = $this->getCompiler()->buildOrderBy($this->orders)) {
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
            foreach ($this->withRelations as $name => $callback) {
                if (is_callable($callback)) {
                    $model->load($name, $callback);
                } else {
                    $model->load($name);
                }
            }
            foreach ($this->withCounts as $name => $callback) {
                if ($rel = $this->getCompiler()->parseRelations($name, $model->relations())) {
                    [$type, $class, $through, $keys, $use] = $rel;
                    $child = new $class();
                    if ($through !== null) {
                        $child->join($through, $keys, 'inner', $model->relations());
                        $keys = $child->throughConditions();
                    }
                    $child->where(function ($where) use ($keys, $result, &$maps) {
                        foreach ($keys as $pk => $fk) {
                            $where->add([$fk => $result[$pk]], IQuery::OPERATOR_IN);
                        }
                    });

                    is_callable($use) and $use($child);
                    is_callable($callback) and $callback($child);

                    $newKeys = [];
                    foreach ($keys as $key => $fk) {
                        $n = $child->getCompiler()->makeColumnAlias($fk, 'pivot_relation');
                        $child->select($fk, $n);
                        $child->groupBy($fk);
                        $newKeys[$key] = trim($n, '`"');
                    }

                    $model->setAttributes([
                        "count_{$name}" => $child->count(),
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
        foreach ($this->withRelations as $name => $relation) {
            if ($rel = $this->getCompiler()->parseRelations($name, $this->getModel()->relations())) {
                [$type, $class, $through, $keys, $use] = $rel;
                $model = new $class();
                $model->setAlias($name);
                $model->select($name . '.*');
                if ($through !== null) {
                    $model->join($through, $keys, 'inner', $this->getModel()->relations());
                    $keys = $model->throughConditions();
                }
                $model->where(function ($where) use ($keys, $results, &$maps) {
                    foreach ($keys as $pk => $fk) {
                        if (!array_key_exists($pk, $maps)) {
                            $maps[$pk] = array_map(function ($m) use ($pk) {
                                return $m[$pk];
                            }, $results);
                        }
                        $where->add([$fk => $maps[$pk]], IQuery::OPERATOR_IN);
                    }
                });
                $newKeys = [];
                foreach ($keys as $key => $fk) {
                    $n = $model->getCompiler()->makeColumnAlias($fk, 'pivot_relation');
                    $model->select($fk, $n);
                    $newKeys[$key] = trim($n, '`"');
                }

                is_callable($use) and $use($model);
                is_callable($relation) and $relation($model);

                $relations[$name] = [
                    'maps' => $newKeys,
                    'type' => $type,
                    'values' => $model->getResults(),
                ];
            }
        }

        $counts = [];
        foreach ($this->withCounts as $name => $relation) {
            if ($rel = $this->getCompiler()->parseRelations($name, $this->getModel()->relations())) {
                [$type, $class, $through, $keys, $use] = $rel;
                $model = new $class();
                $model->select('count(*)', 'count');
                if ($through !== null) {
                    $model->join($through, $keys, 'inner', $this->getModel()->relations());
                    $keys = $model->throughConditions();
                }
                $model->where(function ($where) use ($keys, $results, &$maps) {
                    foreach ($keys as $pk => $fk) {
                        if (!array_key_exists($pk, $maps)) {
                            $maps[$pk] = array_map(function ($m) use ($pk) {
                                return $m[$pk];
                            }, $results);
                        }
                        $where->add([$fk => $maps[$pk]], IQuery::OPERATOR_IN);
                    }
                });

                is_callable($use) and $use($model);
                is_callable($relation) and $relation($model);

                $newKeys = [];
                foreach ($keys as $key => $fk) {
                    $n = $model->getCompiler()->makeColumnAlias($fk, 'pivot_relation');
                    $newKeys[$key] = trim($n, '`"');
                    $model->select($fk, $n);
                    $model->groupBy($fk);
                }

                $query = $model->connection()->execute($model->getQuery(false), $model->getParams());
                $resultCounts = [];
                while ($c = $query->fetch()) {
                    $k = '';
                    foreach ($newKeys as $key) {
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
                        $result['count_' . $name] = $row['value'];
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
    public function groupBy(string $column): IQuery
    {
        $str = $this->getCompiler()->normalizeColumn($column);
        $split = explode('.', $str);
        $this->groups[] = end($split);
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
        int $operator = IQuery::OPERATOR_EQUAL,
        int $separator = IQuery::SEPARATOR_AND
    ): IQuery{
        $this->havingCondition->add($expression, $operator, $separator);
        return $this;
    }

    /**
     * @param  string  $name
     * @param  array   $conditions
     * @param  string  $joinType
     * @param  array   $throughs
     * @return mixed
     */
    public function join(
        string $name,
        array $conditions = [],
        string $joinType = 'inner',
        array $throughs = []
    ): IQuery{
        $relations = count($throughs) > 0 ? $throughs : $this->getModel()->relations();
        if ($rel = $this->getCompiler()->parseRelations($name, $relations)) {
            [$type, $class, $through, $keys, $use] = $rel;
            if (empty($conditions)) {
                $conditions = $keys;
            }
            $sub = new $class();
            $sub->setAlias($name);
            $params = [];
            foreach ($conditions as $pk => $fk) {
                $params[] = $sub->getCompiler()->normalizeColumn($name . '.' . $pk) . ' = ' . $this->getCompiler()->normalizeColumn($fk);
            }
            $this->joins[] = strtoupper($joinType) . ' JOIN ' . $sub->table() . ' AS ' . $this->connection->quote($name) . ' ON ' . implode(' AND ', $params);
            if (count($throughs) > 0) {
                $this->mapThroughConditions = [];
                foreach ($keys as $key => $value) {
                    $this->mapThroughConditions[$key] = $sub->getCompiler()->normalizeColumn($value);
                }
                if ($through !== null) {
                    if ($subrel = $this->getCompiler()->parseRelations($through, $relations)) {
                        [$subtype, $subclass, $subthrough, $subkeys, $subuse] = $subrel;
                        $this->join($through, $this->mapThroughConditions, 'inner', $relations);
                    }
                }
            }
            unset($sub);
        }
        return $this;
    }

    /**
     * @param  int     $size
     * @param  int     $offset
     * @return mixed
     */
    public function limit(int $size, int $offset = 0): IQuery
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
    public function orWhere($expression, int $operator = IQuery::OPERATOR_EQUAL): IQuery
    {
        return $this->where($expression, $operator, IQuery::SEPARATOR_OR);
    }

    /**
     * @param  string  $relation
     * @param  Closure $callback
     * @return mixed
     */
    public function orWhereHas(string $relation, Closure $callback): IQuery
    {
        return $this->whereHas($relation, $callback, IQuery::SEPARATOR_OR);
    }

    /**
     * @param  string  $column
     * @param  int     $sort
     * @return mixed
     */
    public function orderBy(string $column, int $sort = IQuery::ORDER_ASC): IQuery
    {
        $this->orders[] = [
            'column' => $this->getCompiler()->normalizeColumn($column),
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
                $columns[] = $this->connection->quote($key);
                $params = $this->getCompiler()->makeParams([$value]);
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
            $q = 'UPDATE ' . $model->table() . ' AS ' . $this->connection->quote($model->getAlias()) . ' SET ' . implode(', ', $arguments);
            if ($where = $this->getCompiler()->buildCondition($this->whereCondition->all())) {
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
    public function select(string $column, string $alias = null): IQuery
    {
        $column = $this->getCompiler()->normalizeColumn($column);
        $alias = $this->getCompiler()->makeColumnAlias($alias ? $alias : $column);
        $this->selectedColumns[$alias] = $column;
        return $this;
    }

    /**
     * @param  $expression
     * @param  int           $operator
     * @param  int           $separator
     * @return mixed
     */
    public function where(
        $expression,
        int $operator = IQuery::OPERATOR_EQUAL,
        int $separator = IQuery::SEPARATOR_AND
    ): IQuery{
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
        int $separator = IQuery::SEPARATOR_AND
    ): IQuery {
        if ($relation = $this->getCompiler()->parseRelations($name, $this->getModel()->relations())) {
            [$type, $class, $through, $keys, $use] = $relation;
            $model = new $class();
            $model->setAlias($name);
            if ($through !== null) {
                $model->join($through, $keys, 'inner', $this->getModel()->relations());
                $keys = $model->throughConditions();
            }
            $model->where(function (ICondition $where) use ($name, $keys, $model) {
                foreach ($keys as $parent => $child) {
                    $columns = [
                        $model->connection()->quote($child),
                        $this->connection->quote($this->getModel()->getAlias()) . '.' . $this->connection->quote($parent),
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
}
