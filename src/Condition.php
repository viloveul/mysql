<?php

namespace Viloveul\MySql;

use InvalidArgumentException;
use Viloveul\Database\Expression;
use Viloveul\Database\Contracts\Query as IQuery;
use Viloveul\Database\Condition as AbstractCondition;
use Viloveul\Database\Contracts\Expression as IExpression;

class Condition extends AbstractCondition
{
    /**
     * @param $expression
     * @param int           $operator
     * @param int           $separator
     */
    public function add(
        $expression,
        int $operator = IQuery::OPERATOR_EQUAL,
        int $separator = IQuery::SEPARATOR_AND
    ): void {
        if (is_array($expression)) {
            foreach ($expression as $column => $value) {
                $this->push([
                    'separator' => $separator,
                    'argument' => $this->parse($column, $value, $operator),
                ]);
            }
        } elseif (is_callable($expression)) {
            $whereConditions = $this->conditions;
            $this->clear();
            $expression($this);
            if ($compiled = $this->getCompiled()) {
                array_push($whereConditions, [
                    'separator' => $separator,
                    'argument' => '(' . $compiled . ')',
                ]);
                $this->clear();
            }
            $this->conditions = $whereConditions;
        } elseif ($expression instanceof IExpression) {
            $this->push([
                'separator' => $separator,
                'argument' => $expression->getCompiled(),
            ]);
        } else {
            throw new InvalidArgumentException("first argument should type of IExpression or Closure and may be array key-value");
        }
    }

    /**
     * @return mixed
     */
    public function getCompiled(): string
    {
        $condition = '';
        foreach ($this->conditions as $value) {
            if (!empty($condition)) {
                $condition .= ' ' . ($value['separator'] === IQuery::SEPARATOR_AND ? 'AND' : 'OR') . ' ';
            }
            $condition .= $value['argument'];
        }
        return $condition;
    }

    /**
     * @param string   $column
     * @param $value
     * @param int      $operator
     */
    protected function parse(string $column, $value, int $operator): string
    {
        $column = $this->getQuery()->getConnection()->makeNormalizeColumn($column);

        if ($operator === IQuery::OPERATOR_RAW && is_scalar($value)) {
            switch ($value) {
                case IQuery::VALUE_IS_NULL:
                    $argument = "{$column} IS NULL";
                    break;
                case IQuery::VALUE_IS_EMPTY:
                    $argument = "({$column} IS NULL OR {$column} = '')";
                    break;
                case IQuery::VALUE_NOT_NULL:
                    $argument = "{$column} IS NOT NULL";
                    break;
                case IQuery::VALUE_NOT_EMPTY:
                default:
                    $argument = "({$column} IS NOT NULL AND {$column} <> '')";
                    break;
            }
        } else {
            $params = $this->getQuery()->makeParams(is_scalar($value) ? [$value] : ((array) $value));
            switch ($operator) {
                case IQuery::OPERATOR_RANGE:
                case IQuery::OPERATOR_BEETWEN:
                    $first = array_shift($params);
                    $last = isset($params[0]) ? $params[0] : $first;
                    if ($operator === IQuery::OPERATOR_RANGE) {
                        $argument = "({$column} >= {$first} AND {$column} <= $last)";
                    } else {
                        $argument = "({$column} BEETWEN {$first} AND $last)";
                    }
                    break;

                case IQuery::OPERATOR_LIKE:
                    $argument = "{$column} LIKE {$params[0]}";
                    break;
                case IQuery::OPERATOR_EQUAL:
                    $argument = "{$column} = {$params[0]}";
                    break;
                case IQuery::OPERATOR_NOT_EQUAL:
                    $argument = "{$column} <> {$params[0]}";
                    break;
                case IQuery::OPERATOR_GT:
                    $argument = "{$column} > {$params[0]}";
                    break;
                case IQuery::OPERATOR_LT:
                    $argument = "{$column} < {$params[0]}";
                    break;
                case IQuery::OPERATOR_GTE:
                    $argument = "{$column} >= {$params[0]}";
                    break;
                case IQuery::OPERATOR_LTE:
                    $argument = "{$column} <= {$params[0]}";
                    break;

                case IQuery::OPERATOR_IN:
                case IQuery::OPERATOR_NOT_IN:
                default:
                    $op = $operator === IQuery::OPERATOR_NOT_IN ? 'NOT IN' : 'IN';
                    $cond = implode(', ', $params);
                    $argument = "{$column} {$op} ({$cond})";
                    break;
            }
        }
        return $argument;
    }
}
