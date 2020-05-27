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
        $res = $this->dbh->query("SHOW TABLES;")->fetchAll();
        foreach ($res as $row) {
            $this->dbh->exec('DROP TABLE `' . $row[0] . '`');
        }
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
                $keys = $keys[0]['primary_key'] !== '' ? $keys[0]['primary_key'] : $keys[0]['sorting_key'];
                $keys = explode(', ', $keys);
                foreach ($keys as $key => $value) {
                    if (!preg_match('/^[a-zA-Z_][0-9a-zA-Z_]*$/', $value)) {
                        unset($keys[$key]);
                    }
                }
                $primaryKey = $keys;
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

    public function update($table, array $data, array $criteria)
    {
        if (empty($data)) {
            throw new \InvalidArgumentException(
                "Query update can't be prepared without data."
            );
        }
        $set = [];
        foreach ($data as $column => $value) {
            $set[] = $this->getQuotedName($column) . " = ?";
        }
        $where = $this->generateWhereClause($criteria);
        return sprintf('ALTER TABLE %s UPDATE %s %s;', $this->getQuotedName($table), implode(', ', $set), $where);
    }
}
