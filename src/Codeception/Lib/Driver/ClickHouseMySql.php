<?php

namespace Codeception\Lib\Driver;

class ClickHouseMySql extends Db
{
    public function __construct($dsn, $user, $password, $options = null)
    {
        $dsn = str_replace('clickhouse-mysql:', 'mysql:', $dsn);
        parent::__construct($dsn, $user, $password, $options);
    }

    public static function connect($dsn, $user, $password, $options = null)
    {
        $dsn = str_replace('clickhouse-mysql:', 'mysql:', $dsn);
        return parent::connect($dsn, $user, $password, $options);
    }

    public function cleanup()
    {
        $this->dbh->exec('SET FOREIGN_KEY_CHECKS=0;');
        $res = $this->dbh->query("SHOW FULL TABLES WHERE TABLE_TYPE LIKE '%TABLE';")->fetchAll();
        foreach ($res as $row) {
            $this->dbh->exec('drop table `' . $row[0] . '`');
        }
        $this->dbh->exec('SET FOREIGN_KEY_CHECKS=1;');
    }

    protected function sqlQuery($query)
    {
        $this->dbh->exec('SET FOREIGN_KEY_CHECKS=0;');
        parent::sqlQuery($query);
        $this->dbh->exec('SET FOREIGN_KEY_CHECKS=1;');
    }

    public function getQuotedName($name)
    {
        return '`' . str_replace('.', '`.`', $name) . '`';
    }

    /**
     * @param string $tableName
     *
     * @return array[string]
     */
    public function getPrimaryKey($tableName)
    {
        if (!isset($this->primaryKeys[$tableName])) {
            $primaryKey = [];
            $stmt = $this->getDbh()->query(
                'SELECT  primary_key, sorting_key  FROM system.tables  WHERE `name`=\'' . $tableName . '\' LIMIT 1;'
            );
            $keys = $stmt->fetchAll(\PDO::FETCH_ASSOC);
            if ($keys) {
                $keys = $keys[0];
                $primaryKey [] = $keys['primary_key'] !== '' ? $keys['primary_key'] : $keys['sorting_key'];
            }
            $this->primaryKeys[$tableName] = $primaryKey;
        }
        return $this->primaryKeys[$tableName];
    }

    public function deleteQueryByCriteria($table, array $criteria)
    {
        $where = $this->generateWhereClause($criteria);
        $query = 'ALTER TABLE ' . $this->getQuotedName($table) . ' DELETE ' . $where;
        $this->executeQuery($query, array_values($criteria));
    }
}
