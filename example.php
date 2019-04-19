<?php

require __DIR__ . '/vendor/autoload.php';

$db = Viloveul\Database\DatabaseFactory::instance([
    'default' => new Viloveul\MySql\Connection('dev', 'something', 'viloveul_cms', '127.0.0.1', 3306, 'tbl_'),
]);
$db->load();

class User extends Viloveul\Database\Model
{
    public function afterFind(): void
    {
        $this->abc = 'def';
    }

    public function relations(): array
    {
        return [
            'uroles' => [
                'class' => UserRole::class,
                'type' => static::HAS_MANY,
                'keys' => [
                    'id' => 'user_id',
                ],
            ],
        ];
    }

    public function table(): string
    {
        return '{{ user }}';
    }
}

class UserRole extends Viloveul\Database\Model
{
    /**
     * @var array
     */
    protected $protects = [
        // 'user_id',
        'role_id',
    ];

    public function primary()
    {
        return ['user_id', 'role_id'];
    }

    public function table(): string
    {
        return '{{ user_role }}';
    }
}

class Role extends Viloveul\Database\Model
{
    public function table(): string
    {
        return '{{ role }}';
    }
}

$jos = $db->getConnection()->newSchema('wah');
$jos->set('id', Viloveul\Database\Contracts\Schema::TYPE_BIGINT)->increment()->unsigned()->primary();
$jos->set('hhu', Viloveul\Database\Contracts\Schema::TYPE_VARCHAR)->nullable();
$jos->set('hohoho', Viloveul\Database\Contracts\Schema::TYPE_VARCHAR)->nullable();
$jos->run();

$dor = UserRole::getResultOrCreate(['role_id' => 'fajrulaz'], ['user_id' => 'jos']);

dd($dor);

$start = microtime(true);

$ur = User::getResults();

print_r($ur->toArray());

print_r($db->getConnection()->showLogQueries());

echo memory_get_usage() . PHP_EOL;

echo (microtime(true) - $start) . PHP_EOL;
