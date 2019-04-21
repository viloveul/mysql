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
     * @var array
     */
    protected $mappedThroughConditions = [];

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
    protected $selects = [];

    /**
     * @var mixed
     */
    protected $size = 0;

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
        $new = clone $this;
        $new->select('count(*)');
        $query = $new->getConnection()->execute($new->getQuery(false), $new->getParams());
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
        $alias = $this->getConnection()->quote($model->getAlias());
        $q = 'DELETE FROM ' . $alias . ' USING ' . $model->table() . ' AS ' . $alias;
        if ($where = $this->getCompiledWhere()) {
            $q .= " WHERE {$where}";
        }

        try {
            $this->getConnection()->execute($q, $this->getParams());
            return true;
        } catch (Exception $e) {
            throw $e;
        }
    }

    public function getCompiledGroupBy(): string
    {
        return implode(', ', $this->groups);
    }

    /**
     * @return mixed
     */
    public function getCompiledHaving(): string
    {
        return $this->havingCondition->getCompiled();
    }

    public function getCompiledOrderBy(): string
    {
        $compiledOrders = [];
        foreach ($this->orders as $order) {
            $compiledOrders[] = $order['column'] . ' ' . ($order['sort'] === IQuery::SORT_ASC ? 'ASC' : 'DESC');
        }
        return implode(', ', $compiledOrders);
    }

    /**
     * @return mixed
     */
    public function getCompiledSelect(): string
    {
        $selects = [];
        if (empty($this->selects)) {
            return $this->getConnection()->quote($this->getModel()->getAlias()) . '.*';
        }
        foreach ($this->selects as $alias => $select) {
            if (strpos($select, '*') !== false && strpos($select, '(') === false) {
                $mine = $select;
            } else {
                $mine = ($alias == $select) ? $select : "{$select} AS {$alias}";
            }
            if (strpos($mine, '(') !== false) {
                array_unshift($selects, $mine);
            } else {
                array_push($selects, $mine);
            }
        }
        return implode(', ', array_unique($selects));
    }

    /**
     * @return mixed
     */
    public function getCompiledTable(): string
    {
        $q = $this->getModel()->table() . ' AS ' . $this->getConnection()->quote($this->getModel()->getAlias());

        if (count($this->joins) > 0) {
            $q .= ' ' . implode(' ', $this->joins);
        }
        return $q;
    }

    /**
     * @return mixed
     */
    public function getCompiledWhere(): string
    {
        return $this->whereCondition->getCompiled();
    }

    /**
     * @param bool $compile
     */
    public function getQuery(bool $compile = true): string
    {
        $q = 'SELECT ' . $this->getCompiledSelect();
        $q .= ' FROM ' . $this->getCompiledTable();

        if ($where = $this->getCompiledWhere()) {
            $q .= " WHERE {$where}";
        }
        if ($groups = $this->getCompiledGroupBy()) {
            $q .= ' GROUP BY ' . $groups;
        }
        if ($having = $this->getCompiledHaving()) {
            $q .= " HAVING {$having}";
        }
        if ($order = $this->getCompiledOrderBy()) {
            $q .= ' ORDER BY ' . $order;
        }
        if ($this->size > 0) {
            $q .= ' LIMIT ' . $this->size . ' OFFSET ' . abs($this->offset);
        }
        return $compile ? $this->getConnection()->prepare($q) : $q;
    }

    /**
     * @return mixed
     */
    public function getResult()
    {
        $new = clone $this;
        $new->limit(1, 0);
        $new->getModel()->beforeFind();
        $query = $new->getConnection()->execute($new->getQuery(false), $new->getParams());
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
                if ($rel = $this->parseRelations($name, $model->relations())) {
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
                        $n = $child->getConnection()->makeAliasColumn($fk, 'pivot_relation');
                        $child->select($fk, $n);
                        $child->groupBy($n);
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
     * @return mixed
     */
    public function getResults(): ICollection
    {
        $maps = [];
        $relations = [];
        $this->getModel()->beforeFind();
        $query = $this->getConnection()->execute(
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
            if ($rel = $this->parseRelations($name, $this->getModel()->relations())) {
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
                    $n = $model->getConnection()->makeAliasColumn($fk, 'pivot_relation');
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
            if ($rel = $this->parseRelations($name, $this->getModel()->relations())) {
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
                    $n = $model->getConnection()->makeAliasColumn($fk, 'pivot_relation');
                    $newKeys[$key] = trim($n, '`"');
                    $model->select($fk, $n);
                    $model->groupBy($n);
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
     * @param string     $column
     * @param $default
     */
    public function getValue(string $column, $default = null)
    {
        $new = clone $this;
        $new->limit(1, 0);
        $new->getModel()->beforeFind();
        $query = $new->getConnection()->execute($new->getQuery(false), $new->getParams());
        $result = $query->fetch();
        return array_key_exists($column, $result) ? $result[$column] : $default;
    }

    /**
     * @param  string  $column
     * @return mixed
     */
    public function groupBy(string $column): IQuery
    {
        $str = $this->getConnection()->makeNormalizeColumn($column);
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
        if ($rel = $this->parseRelations($name, $relations)) {
            [$type, $class, $through, $keys, $use] = $rel;
            if (empty($conditions)) {
                $conditions = $keys;
            }
            $sub = new $class();
            $sub->setAlias($name);
            $params = [];
            foreach ($conditions as $pk => $fk) {
                $params[] = $sub->getConnection()->makeNormalizeColumn($pk, $name) . ' = ' . $this->getConnection()->makeNormalizeColumn($fk, $this->getModel()->getAlias());
            }
            $this->joins[] = strtoupper($joinType) . ' JOIN ' . $sub->table() . ' AS ' . $this->getConnection()->quote($name) . ' ON ' . implode(' AND ', $params);
            if (count($throughs) > 0) {
                $this->mappedThroughConditions = [];
                foreach ($keys as $key => $value) {
                    $this->mappedThroughConditions[$key] = $sub->getConnection()->makeNormalizeColumn($value, $name);
                }
                if ($through !== null) {
                    if ($subrel = $this->parseRelations($through, $relations)) {
                        [$subtype, $subclass, $subthrough, $subkeys, $subuse] = $subrel;
                        $this->join($through, $this->mappedThroughConditions, 'inner', $relations);
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
     * @param string  $name
     * @param Closure $callback
     */
    public function load(string $name, Closure $callback = null): void
    {
        if ($rel = $this->parseRelations($name, $this->getModel()->relations())) {
            [$type, $class, $through, $keys, $use] = $rel;
            $model = new $class();
            $model->setAlias($name);
            if ($through !== null) {
                $model->join($through, $keys, 'inner', $this->getModel()->relations());
                $keys = $model->throughConditions();
            }
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
     * @param  array   $params
     * @return mixed
     */
    public function makeParams(array $params): array
    {
        $binds = [];
        foreach ($params as $value) {
            $binds[] = $this->addParam($value);
        }
        return $binds;
    }

    /**
     * @param string $column
     */
    public function max(string $column)
    {
        $new = clone $this;
        $new->select("max({$column})");
        $query = $new->getConnection()->execute($new->getQuery(false), $new->getParams());
        return $query->fetchColumn();
    }

    /**
     * @param string $column
     */
    public function min(string $column)
    {
        $new = clone $this;
        $new->select("min({$column})");
        $query = $new->getConnection()->execute($new->getQuery(false), $new->getParams());
        return $query->fetchColumn();
    }

    /**
     * @return mixed
     */
    public function newCondition(): ICondition
    {
        $condition = new Condition();
        $condition->setQuery($this);
        return $condition;
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
    public function orderBy(string $column, int $sort = IQuery::SORT_ASC): IQuery
    {
        $this->orders[] = [
            'column' => $this->getConnection()->makeNormalizeColumn($column),
            'sort' => $sort,
        ];
        return $this;
    }

    /**
     * @param  string  $name
     * @param  array   $relations
     * @return mixed
     */
    public function parseRelations(string $name, array $relations): array
    {
        if (array_key_exists($name, $relations)) {
            $relation = $relations[$name];
            $def = ['type' => null, 'class' => null, 'through' => null, 'keys' => [], 'use' => null];
            $resolve = [];
            foreach ($def as $key => $value) {
                $resolve[] = array_key_exists($key, $relation) ? $relation[$key] : $value;
            }
            return $resolve;
        } else {
            return [];
        }
    }

    /**
     * @return mixed
     */
    public function save(): IModel
    {
        $model = $this->getModel();
        $attributes = $model->getAttributes();
        $stateInsert = $model->isNewRecord();
        $columns = [];
        $values = [];
        $duplicates = [];
        foreach ($attributes as $key => $value) {
            if (!($value instanceof IModel) && !$model->isAttributeCount($key)) {
                $columns[] = $this->getConnection()->quote($key);
                $params = $this->makeParams([$value]);
                $values[] = $params[0];
            }
        }

        $model->beforeSave();

        if ($stateInsert === true) {
            $q = 'INSERT INTO ' . $model->table();
            $q .= '(' . implode(', ', $columns) . ')';
            $q .= ' VALUES(' . implode(', ', $values) . ')';
            $q .= ' ON DUPLICATE KEY UPDATE';
            foreach ($columns as $key => $column) {
                $duplicates[] = " {$column} = {$values[$key]}";
            }
            $q .= implode(', ', $duplicates);
        } else {
            $stateInsert = false;
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
            $q = 'UPDATE ' . $model->table() . ' AS ' . $this->getConnection()->quote($model->getAlias()) . ' SET ' . implode(', ', $arguments);
            if ($where = $this->getCompiledWhere()) {
                $q .= " WHERE {$where}";
            }
        }

        try {
            $this->getConnection()->execute($q, $this->getParams());
        } catch (Exception $e) {
            throw $e;
        }

        $pri = $model->primary();
        if ($stateInsert === true) {
            $id = $this->getConnection()->lastInsertedValue(is_scalar($pri) ? $pri : null);
            if (abs($id) > 0 && is_scalar($pri)) {
                $attributes[$pri] = $id;
            }
        }
        $pris = is_scalar($pri) ? [$pri] : (array) $pri;
        $new = $model->newInstance();
        foreach ($pris as $mine) {
            if (array_key_exists($mine, $attributes)) {
                $new->where([$mine => $attributes[$mine]]);
            }
        }
        $result = $new->getResult();
        $model->resetState();
        $model->clearAttributes();
        $model->setAttributes($result->toArray());
        $model->afterSave();
        unset($result);
        return $model;
    }

    /**
     * @param  $column
     * @param  string    $alias
     * @return mixed
     */
    public function select($column, string $alias = null): IQuery
    {
        if (!is_scalar($column)) {
            foreach ($column as $key => $value) {
                $this->select($value, is_numeric($key) ? $key : null);
            }
        } else {
            $column = $this->getConnection()->makeNormalizeColumn($column, $this->getModel()->getAlias());
            $alias = $this->getConnection()->makeAliasColumn($alias ? $alias : $column);
            $this->selects[$alias] = $column;
        }
        return $this;
    }

    /**
     * @param string $name
     * @param array  $values
     * @param int    $mode
     */
    public function sync(string $name, array $values, int $mode = IQuery::SYNC_BOTH): void
    {
        $model = $this->getModel();
        if ($relation = $this->parseRelations($name, $model->relations())) {
            [$type, $class, $through, $keys, $use] = $relation;
            $top = new $class();
            $subpri = false;
            $maps = [];
            foreach ($keys as $pk => $fk) {
                $maps[$fk] = $model->{$pk};
            }
            $subpris = array_filter((array) $top->primary(), function ($v) use ($maps) {
                return !array_key_exists($v, $maps);
            });
            $subpri = end($subpris);
            if ($mode !== IQuery::SYNC_ATTACH) {
                $top->where($maps);
                if (!empty($values)) {
                    if ($mode === IQuery::SYNC_BOTH) {
                        $top->where([$subpri => $values], IQuery::OPERATOR_NOT_IN);
                    } else {
                        $top->where([$subpri => $values], IQuery::OPERATOR_IN);
                    }
                }
                $top->delete();
                $top->resetState();
            }

            if ($mode !== IQuery::SYNC_DETACH && !empty($values)) {
                $model->load($name);
                $objectCurrents = $model->{$name};
                $currents = [];
                if ($objectCurrents !== null) {
                    foreach ($objectCurrents as $osub) {
                        $currents[] = $osub->{$subpri};
                    }
                }
                $topParams = $top->makeParams($maps);
                $sqlParams = $topParams;
                $sqlValues = [];
                $sqlColumns = [];
                foreach ($keys as $key) {
                    $sqlColumns[] = $top->getConnection()->quote($key);
                }
                $sqlColumns[] = $top->getConnection()->quote($subpri);
                foreach ($values as $value) {
                    if (!in_array($value, $currents)) {
                        $params = $top->makeParams([$value]);
                        $sqlValues[] = '(' . implode(', ', array_merge($topParams, $params)) . ')';
                        $sqlParams[] = $params[0];
                    }
                }
                try {
                    if (!empty($sqlValues)) {
                        $sql = 'INSERT INTO ' . $top->table();
                        $sql .= ' (' . implode(', ', $sqlColumns) . ') ';
                        $sql .= 'VALUES ' . implode(', ', $sqlValues);
                        $top->getConnection()->execute($sql, $top->getParams());
                    }
                } catch (Exception $e) {
                    throw $e;
                }
            }
            $model->resetState();
            $top->resetState();
        }
    }

    /**
     * @return mixed
     */
    public function throughConditions(): array
    {
        return $this->mappedThroughConditions;
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
        if ($relation = $this->parseRelations($name, $this->getModel()->relations())) {
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
                        $this->getConnection()->quote($this->getModel()->getAlias()) . '.' . $this->getConnection()->quote($parent),
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
