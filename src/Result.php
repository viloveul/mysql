<?php

namespace Viloveul\MySql;

use Viloveul\Database\Contracts\Result as IResult;

class Result implements IResult
{
    /**
     * @var mixed
     */
    protected $query;

    /**
     * @param $query
     */
    public function __construct($query)
    {
        $this->query = $query;
    }

    /**
     * @return mixed
     */
    public function fetchAll(): array
    {
        $results = [];
        if (method_exists($this->query, 'fetchAll')) {
            $results = $this->query->fetchAll();
        } else {
            while ($result = $this->query->fetch()) {
                $results[] = $result;
            }
        }
        return $results;
    }

    /**
     * @return mixed
     */
    public function fetchOne()
    {
        return $this->query->fetch();
    }

    /**
     * @param  int        $column
     * @param  $default
     * @return mixed
     */
    public function fetchScalar(int $column = 0, $default = null)
    {
        return $this->query->fetchColumn($column) ?: $default;
    }
}
