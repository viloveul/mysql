<?php

namespace Viloveul\MySql;

use Viloveul\Database\Contracts\Compiler as ICompiler;
use Viloveul\Database\Contracts\QueryBuilder as IQueryBuilder;

class Compiler implements ICompiler
{
    /**
     * @var mixed
     */
    private $builder;

    /**
     * @param IQueryBuilder $builder
     */
    public function __construct(IQueryBuilder $builder)
    {
        $this->builder = $builder;
    }

    /**
     * @return mixed
     */
    public function buildCondition(array $conditions): string
    {
        $condition = '';
        foreach ($conditions as $value) {
            if (!empty($condition)) {
                $condition .= ' ' . ($value['separator'] === IQueryBuilder::SEPARATOR_AND ? 'AND' : 'OR') . ' ';
            }
            $condition .= $value['argument'];
        }
        return $condition;
    }

    /**
     * @param array $groups
     */
    public function buildGroupBy(array $groups): string
    {
        return implode(', ', $groups);
    }

    /**
     * @param array $orders
     */
    public function buildOrderBy(array $orders): string
    {
        $compiledOrders = [];
        foreach ($orders as $order) {
            $compiledOrders[] = $order['column'] . ' ' . ($order['sort'] === IQueryBuilder::ORDER_ASC ? 'ASC' : 'DESC');
        }
        return implode(', ', $compiledOrders);
    }

    /**
     * @param array $selectedColumns
     */
    public function buildSelectedColumn(array $selectedColumns): string
    {
        $selects = [];
        if (empty($selectedColumns)) {
            return '*';
        }
        if (!in_array('*', $selectedColumns)) {
            foreach ($selectedColumns as $alias => $select) {
                $selects[] = ($alias == $select) ? $select : "{$select} AS {$alias}";
            }
        }
        return implode(', ', array_unique($selects));
    }

    /**
     * @param  string  $column
     * @return mixed
     */
    public function makeColumnAlias(string $column): string
    {
        if (is_numeric($column)) {
            return $column;
        } else {
            preg_match_all('~\`([a-zA-Z0-9\_]+)\`~mi', $column, $matches);
            if (array_key_exists(1, $matches) && count($matches[1]) > 0) {
                return $this->builder->quote(end($matches[1]));
            } else {
                return $this->builder->quote(preg_replace('/[^a-zA-Z0-9\_\.]+/', '', $column));
            }
        }
    }

    /**
     * @param  array   $params
     * @return mixed
     */
    public function makeParams(array $params): array
    {
        $binds = [];
        foreach ($params as $value) {
            $binds[] = $this->builder->addParam($value);
        }
        return $binds;
    }

    /**
     * @param  string  $column
     * @return mixed
     */
    public function normalizeColumn(string $column): string
    {
        if (is_numeric($column)) {
            return $column;
        } elseif (strpos($column, '(') !== false && strpos($column, ')') !== false) {
            return preg_replace_callback('#(\w+)\(([a-zA-Z0-9\_\.\`\"]+)\)#', function ($match) {
                $exploded = explode('.', $match[2]);
                if (count($exploded) === 1) {
                    array_unshift($exploded, $this->builder->getModel()->getAlias());
                }
                $parts = array_map([$this->builder, 'quote'], $exploded);
                return $match[1] . '(' . implode('.', $parts) . ')';
            }, $column);
        } else {
            $exploded = explode('.', $column);
            if (count($exploded) === 1) {
                array_unshift($exploded, $this->builder->getModel()->getAlias());
            }
            $parts = array_map([$this->builder, 'quote'], $exploded);
            return implode('.', $parts);
        }
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
}
