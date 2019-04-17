<?php

namespace Viloveul\MySql;

use Viloveul\MySql\Compiler;
use InvalidArgumentException;
use Viloveul\Database\Expression;
use Viloveul\Database\Contracts\Compiler as ICompiler;
use Viloveul\Database\Contracts\Condition as ICondition;
use Viloveul\Database\Contracts\Expression as IExpression;
use Viloveul\Database\Contracts\QueryBuilder as IQueryBuilder;

class Condition implements ICondition
{
    /**
     * @var mixed
     */
    protected $builder;

    /**
     * @var mixed
     */
    protected $compiler;

    /**
     * @var array
     */
    protected $conditions = [];

    /**
     * @param IQueryBuilder $builder
     * @param ICompiler     $compiler
     */
    public function __construct(IQueryBuilder $builder, ICompiler $compiler)
    {
        $this->builder = $builder;
        $this->compiler = $compiler;
    }

    /**
     * @param $expression
     * @param int           $operator
     * @param int           $separator
     */
    public function add(
        $expression,
        int $operator = IQueryBuilder::OPERATOR_EQUAL,
        int $separator = IQueryBuilder::SEPARATOR_AND
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
            if ($compiled = $this->compiler->buildCondition($this->conditions)) {
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
    public function all(): array
    {
        return $this->conditions;
    }

    public function clear(): void
    {
        foreach ($this->conditions as $key => $value) {
            $this->conditions[$key] = null;
            unset($this->conditions[$key]);
        }
        $this->conditions = [];
    }

    /**
     * @param array $condition
     */
    public function push(array $condition): void
    {
        $this->conditions[] = $condition;
    }

    /**
     * @param string   $column
     * @param $value
     * @param int      $operator
     */
    protected function parse(string $column, $value, int $operator): string
    {
        $column = $this->compiler->normalizeColumn($column);
        $params = $this->compiler->makeParams(is_scalar($value) ? [$value] : ((array) $value));
        switch ($operator) {
            case IQueryBuilder::OPERATOR_RANGE:
            case IQueryBuilder::OPERATOR_BEETWEN:
                $first = array_shift($params);
                $last = isset($params[0]) ? $params[0] : $first;
                if ($operator === IQueryBuilder::OPERATOR_RANGE) {
                    $argument = "({$column} >= {$first} AND {$column} <= $last)";
                } else {
                    $argument = "({$column} BEETWEN {$first} AND $last)";
                }
                break;

            case IQueryBuilder::OPERATOR_LIKE:
                $argument = "{$column} LIKE {$params[0]}";
                break;
            case IQueryBuilder::OPERATOR_EQUAL:
                $argument = "{$column} = {$params[0]}";
                break;
            case IQueryBuilder::OPERATOR_GT:
                $argument = "{$column} > {$params[0]}";
                break;
            case IQueryBuilder::OPERATOR_LT:
                $argument = "{$column} < {$params[0]}";
                break;
            case IQueryBuilder::OPERATOR_GTE:
                $argument = "{$column} >= {$params[0]}";
                break;
            case IQueryBuilder::OPERATOR_LTE:
                $argument = "{$column} <= {$params[0]}";
                break;

            case IQueryBuilder::OPERATOR_IN:
            case IQueryBuilder::OPERATOR_NOT_IN:
            default:
                $op = $operator === IQueryBuilder::OPERATOR_NOT_IN ? 'NOT IN' : 'IN';
                $cond = implode(', ', $params);
                $argument = "{$column} {$op} ({$cond})";
                break;
        }
        return $argument;
    }
}
