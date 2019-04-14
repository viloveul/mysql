<?php

namespace Viloveul\MySql;

use ArrayIterator;
use Viloveul\Database\Contracts\Collection as ICollection;

class Collection implements ICollection
{
    /**
     * @var array
     */
    protected $results = [];

    /**
     * @param array $results
     */
    public function __construct(array $results)
    {
        $this->results = $results;
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
