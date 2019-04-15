<?php

namespace Viloveul\MySql;

use ArrayIterator;
use Viloveul\Database\Contracts\Model as IModel;
use Viloveul\Database\Contracts\Collection as ICollection;

class Collection implements ICollection
{
    /**
     * @var array
     */
    protected $results = [];

    /**
     * @param  IModel  $model
     * @param  array   $results
     * @param  array   $relations
     * @return mixed
     */
    public function __construct(IModel $model, array $results, array $relations)
    {
        $this->results = array_map(function ($result) use ($model, $relations) {
            $new = clone $model;
            $new->setAttributes($result);
            $filter = array_filter($relations, function ($v) use ($result) {
                return $result[$v['pk']] == $v['identifier'];
            });
            $new->setAttributes(array_map(function ($v) {
                return $v['values'];
            }, $filter));
            return $new;
        }, $results);
    }

    /**
     * @return mixed
     */
    public function all(): array
    {
        return $this->results;
    }

    public function getIterator()
    {
        return new ArrayIterator($this->all());
    }

    /**
     * @return mixed
     */
    public function jsonSerialize()
    {
        return $this->toArray();
    }

    /**
     * @return mixed
     */
    public function toArray(): array
    {
        return $this->all();
    }
}
