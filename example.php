<?php

error_reporting(-1);
ini_set('display_errors', 'On');

require __DIR__ . '/vendor/autoload.php';

$db = Viloveul\Database\DatabaseFactory::instance([
    'default' => new Viloveul\MySql\Connection('dev', 'something', 'sample', '127.0.0.1', 3306, 'tbl_'),
]);
$db->load();

class RoleChild extends Viloveul\Database\Model
{
    public function primary()
    {
        return ['role_id', 'child_id'];
    }

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

class UserRole extends Viloveul\Database\Model
{
    public function relations(): array
    {
        return [
            'roles' => [
                'class' => Role::class,
                'type' => static::HAS_MANY,
                'keys' => [
                    'role_id' => 'id',
                ],
            ],
            'users' => [
                'class' => User::class,
                'type' => static::HAS_MANY,
                'keys' => [
                    'user_id' => 'id',
                ],
            ],
        ];
    }

    public function table(): string
    {
        return '{{ user_role }}';
    }
}

class User extends Viloveul\Database\Model
{
    public function table(): string
    {
        return '{{ user }}';
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
                'through' => 'childRelations',
                'keys' => [
                    'child_id' => 'id',
                ],
            ],
        ];
    }

    public function table(): string
    {
        return '{{ pegawai }}';
    }
}

// $r = Role::newInstance();
// $r->setAttributes([
//     'name' => 'fajrulaz',
//     'type' => 'b',
//     'id' => mt_rand()
// ]);
// $r->save();

// dd($r->toArray());

// // $db->getConnection()->transaction();
// // $ur = RoleChild::where(['child_id' => '00296529-1877-43c4-ae7c-31d5f0e8df95'])->getResult();
// // $ur->role_id = '00296529-1877-43c4-ae7c-31d5f0e8df90';
// dd($ur);

// $jos = $db->getConnection()->newSchema('wah');
// $jos->set('id', Viloveul\Database\Contracts\Schema::TYPE_BIGINT)->increment()->unsigned()->primary();
// $jos->set('hhu', Viloveul\Database\Contracts\Schema::TYPE_VARCHAR)->nullable();
// $jos->set('hohoho', Viloveul\Database\Contracts\Schema::TYPE_VARCHAR)->nullable();
// $jos->run();

// $x = RoleChild::join('childs', ['id' => 'role_id'], 'right')->getQuery();
// dd($x);

$start = microtime(true);

$dor = Role::select('id')->groupBy('id')->count();

dump($dor);
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
