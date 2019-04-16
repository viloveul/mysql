<?php

require __DIR__ . '/vendor/autoload.php';

Viloveul\Database\DatabaseFactory::instance([
    'default' => new Viloveul\MySql\Connection('host=127.0.0.1;dbname=viloveul_cms', 'dev', 'something', 'tbl_'),
]);

class User extends Viloveul\Database\Model
{
    public function relations(): array
    {
        return [
            'uroles' => [
                'class' => UserRole::class,
                'type' => static::HAS_ONE,
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

$ur = User::with('uroles')->getResults();

dd($ur);
