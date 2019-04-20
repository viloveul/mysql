<?php

namespace Viloveul\MySql;

use Viloveul\Database\Contracts\Query as IQuery;
use Viloveul\Database\Compiler as AbstractCompiler;

class Compiler extends AbstractCompiler
{
    /**
     * @param array $orders
     */
    public function buildOrderBy(array $orders): string
    {
        $compiledOrders = [];
        foreach ($orders as $order) {
            $compiledOrders[] = $order['column'] . ' ' . ($order['sort'] === IQuery::ORDER_ASC ? 'ASC' : 'DESC');
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
            return $this->connection->quote($this->builder->getModel()->getAlias()) . '.*';
        }
        foreach ($selectedColumns as $alias => $select) {
            if (strpos($select, '*') !== false && strpos($selects, '(') === false) {
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
     * @param  string  $column
     * @return mixed
     */
    public function makeColumnAlias(string $column, string $append = null): string
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
            return $this->connection->quote($new);
        }
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
            return preg_replace_callback('#(\w+)\(([a-zA-Z0-9\_\.\`\"\*]+)\)#', function ($match) {
                $exploded = explode('.', $match[2]);
                if (count($exploded) === 1 && strpos($match[2], '*') === false) {
                    array_unshift($exploded, $this->builder->getModel()->getAlias());
                }
                $parts = array_map([$this->connection, 'quote'], $exploded);
                return $match[1] . '(' . implode('.', $parts) . ')';
            }, $column);
        } else {
            $exploded = explode('.', preg_replace('/[^a-zA-Z0-9\.\_\*]+/', '', $column));
            if (count($exploded) === 1) {
                array_unshift($exploded, $this->builder->getModel()->getAlias());
            }
            $parts = array_map([$this->connection, 'quote'], $exploded);
            return implode('.', $parts);
        }
    }
}
