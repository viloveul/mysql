<?php

require __DIR__ . '/vendor/autoload.php';

$db = Viloveul\Database\DatabaseFactory::instance([
    'default' => new Viloveul\MySql\Connection('host=127.0.0.1;dbname=viloveul_cms', 'dev', 'something', 'tbl_'),
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

$start = microtime(true);

$ur = User::getResults();

echo print_r($ur->toArray());

print_r($db->getConnection()->showLogQueries());

echo memory_get_usage() . PHP_EOL;

echo (microtime(true) - $start) . PHP_EOL;
