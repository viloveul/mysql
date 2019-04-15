<?php

require __DIR__ . '/vendor/autoload.php';

$db = Viloveul\Database\DatabaseFactory::instance([
    'default' => new Viloveul\MySql\Connection('host=127.0.0.1;dbname=viloveul_cms', 'dev', 'something', 'tbl_'),
]);

class User extends Viloveul\Database\Model
{
    public function relations(): array
    {
        return [
            'uroles' => [static::HAS_MANY, UserRole::class, 'id', 'user_id'],
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

$ur = UserRole::getResults();
// $ur->user_id = 'dorrdr';
dd($ur);
