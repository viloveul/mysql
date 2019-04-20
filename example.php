<?php

require __DIR__ . '/vendor/autoload.php';

$db = Viloveul\Database\DatabaseFactory::instance([
    'default' => new Viloveul\MySql\Connection('dev', 'something', 'viloveul_cms', '127.0.0.1', 3306, 'tbl_'),
]);
$db->load();

class RoleChild extends Viloveul\Database\Model
{
    public function relations(): array
    {
        return [
            'childs' => [
                'class' => Role::class,
                'type' => static::HAS_MANY,
                'keys' => [
                    'child_id' => 'id',
                ],
            ],
        ];
    }

    public function table(): string
    {
        return '{{ role_child }}';
    }
}

class Role extends Viloveul\Database\Model
{
    public function relations(): array
    {
        return [
            'childRelations' => [
                'class' => RoleChild::class,
                'type' => static::HAS_MANY,
                'keys' => [
                    'id' => 'role_id',
                ],
            ],
            'subRelations' => [
                'class' => RoleChild::class,
                'type' => static::HAS_MANY,
                'through' => 'childRelations',
                'keys' => [
                    'child_id' => 'role_id',
                ],
            ],
            'childs' => [
                'class' => __CLASS__,
                'type' => static::HAS_MANY,
                'through' => 'subRelations',
                'keys' => [
                    'child_id' => 'id',
                ],
            ],
        ];
    }

    public function table(): string
    {
        return '{{ role }}';
    }
}

// $jos = $db->getConnection()->newSchema('wah');
// $jos->set('id', Viloveul\Database\Contracts\Schema::TYPE_BIGINT)->increment()->unsigned()->primary();
// $jos->set('hhu', Viloveul\Database\Contracts\Schema::TYPE_VARCHAR)->nullable();
// $jos->set('hohoho', Viloveul\Database\Contracts\Schema::TYPE_VARCHAR)->nullable();
// $jos->run();

// $x = RoleChild::join('childs', ['id' => 'role_id'], 'right')->getQuery();
// dd($x);

$start = microtime(true);
$dor = Role::withCount('childs')->withCount('childRelations')
    ->where(
        ['id' => [
            '017fb6c4-8cf3-4ee5-9982-01d940632472',
            '0eec0533-c7d9-4783-9a9b-5772fff2d786',
        ]],
        Viloveul\Database\Contracts\Query::OPERATOR_IN
    )
    ->getResults();

dump($dor->toArray());
// exit;
// //     // dd($dor);
// // foreach ($dor as $key => $value) {
// //     dump($value->name, $value->childs);
// // }

// $u = Role::withCount('childs')->getResult();
// dd($u);

print_r($db->getConnection()->showLogQueries());
// exit;

// $ur = User::getResults();

// print_r($ur->toArray());

echo memory_get_usage() . PHP_EOL;

echo (microtime(true) - $start) . PHP_EOL;
