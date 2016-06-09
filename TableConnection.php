<?php

namespace iRAP\Mysqli;

class TableConnection
{
    private $m_tableName;
    private $m_columns;
    private $m_mysqli;
    private $m_primaryKey;
    private $m_chunkSize; # number of rows to operate on at a time.

    
    /**
     * Factory method to create a TableConnection more simply through the use of an already existing 
     * DatabaseConnection object.
     * @param DatabaseConnection $conn
     * @param string $table
     * @param int $chunkSize - max number of rows to retrieve/insert at a time.
     */
    public function __construct(DatabaseConnection $conn, $table, $chunkSize=5000)
    {
        $this->m_tableName = $table;        
        $this->m_mysqli = $conn->getMysqli();
        
        $this->fetchPrimaryKey();
        $this->fetchColumns();
        $this->m_chunkSize = $chunkSize;
    }
    
    
    /**
     * Fetches the rows from the table with the index being the hash and the value being an array
     * that forms the primary key value.
     * @return array - assoc array of row-hash/primary key pairs.
     */
    public function fetchHashMap()
    {
        $rows = array();
        
        $wrapped_column_list = \iRAP\CoreLibs\ArrayLib::wrapElements($this->m_columns, "`");
        
        $offset = 0;
        
        $sql = 
            "SELECT " . $this->getPrimaryKeyString() . ", " .
            "MD5( CONCAT_WS('#'," . implode(',', $wrapped_column_list) . ")) as hash " . 
            "FROM `" . $this->m_tableName . "` ";

        $result = $this->m_mysqli->query($sql);

        while (($row = $result->fetch_assoc()) != null)
        {
            $primary_key_value = array();
            foreach ($this->m_primaryKey as $column_name)
            {
                $primary_key_value[] = $row[$column_name];
            }

            $rows[$row['hash']] = $primary_key_value;
        }

        $offset += $this->m_chunkSize;
        
        return $rows;
    }
    
    
    /**
     * Fetch all the rows that have the specified primary key values
     * @param Array $primaryKeyValues - array of keys where each key is an array because keys may 
     * be formed of multiple columns
     * @return type
     */
    public function fetchRows($primaryKeyValues)
    {        
        $rows = array();
        
        if (count($primaryKeyValues) > 0)
        {
            $primary_key_chunks = array_chunk($primaryKeyValues, $this->m_chunkSize);
            
            foreach ($primary_key_chunks as $primary_key_value_set)
            {
                $key_value_sets = array();
            
                foreach ($primary_key_value_set as $index => $set)
                {
                    $quoted_set = \iRAP\CoreLibs\ArrayLib::wrapElements($set, "'");
                    $key_value_sets[] = "(" . implode(',', $quoted_set) . ")";
                }
                
                
                $sql = "SELECT * FROM `" . $this->m_tableName . "` " .
                       "WHERE (" . $this->getPrimaryKeyString() . ") IN (" . implode(',', $key_value_sets) . ")";

                $result = $this->m_mysqli->query($sql);

                while (($row = $result->fetch_assoc()) != null)
                {
                    $rows[] = $row;
                }
            }
        }
        
        return $rows;
    }
    
    
    /**
     * Fetches all of the data from the database.
     * WARNING - This could potentially be a huge memory hog!
     * @param void
     * @return type
     */
    public function fetchAllRows()
    {
        $rows = array();
         
        $sql = "SELECT * FROM `" . $this->m_tableName . "`";
        $result = $this->m_mysqli->query($sql);

        /* @var $result \mysqli_result */
        while (($row = $result->fetch_assoc()) != null)
        {
            $rows[] = $row;
        }
        
        return $rows;
    }
    
    
    /**
     * Fetches a range of data from the table.
     * @param int $start - the starting position (this does not need there to be an id integer column)
     * @param int $num_rows - the number of rows you would like to fetch. This method will return less than this
     *                        if there are not that many rows left in the table.
     * @return Array - associative array of the results.
     */
    public function fetchRange($start, $num_rows)
    {
        $rows = array();

        $sql = "SELECT * FROM `" . $this->m_tableName . "` " .
               "LIMIT " . $num_rows . ' OFFSET ' . $start;
        
        $result = $this->m_mysqli->query($sql);
        
        /* @var $result \mysqli_result */
        while (($row = $result->fetch_assoc()) != null)
        {
            $rows[] = $row;
        }
        
        return $rows;
    }
    
    
    /**
     * Fetches all the primary key values in the table. (not the name of the primary key)
     * The result will be left in array form because more than one column may represent the key!
     * @return Array - list of all the primary key values (usually this is just an array of one, but since multiple
     *                 values can form the primary key, this could be an array of multiple values)
     */
    public function fetchPrimaryKeyValues()
    {
        $values = array();
        $sql = "SELECT " . $this->getPrimaryKeyString() . " FROM `" . $this->m_tableName . "`";
        $resultSet = $this->m_mysqli->query($sql);
        
        if ($resultSet === FALSE)
        {
            throw new \Exception("query failed: [" . $sql . ']');
        }
        
        /* @var $resultSet mysqli_result */
        while (($row = $resultSet->fetch_array(MYSQLI_NUM)) != null) # fetches only numerical rather than assoc as well
        {
            $values[] = $row;
        }
        
        return $values;
    }
    
    
    /**
     * Trys to insert the row (column-name/valu pairs) into this table. 
     * @param Array $rows - assoc array of column names to values to insert.
     * @return void
     */
    public function insertRows($rows)
    {
        $chunks = array_chunk($rows, $this->m_chunkSize, $preserve_keys=true);
        
        # Use multi query if the rows are going to vary in structure, but they really shouldnt.
        $USE_MULTI_QUERY = FALSE;
        
        if ($USE_MULTI_QUERY)
        {
            foreach ($chunks as $row_set)
            {
                $multiQuery = new iRAP\MultiQuery\MultiQuery($this->m_mysqli);
                
                foreach ($row_set as $row)
                {
                    $query = 
                        "INSERT INTO `" . $this->m_tableName . "` " .
                        "SET " . iRAP\CoreLibs\Core::generateMysqliEscapedPairs($row, $this->m_mysqli);

                    $multiQuery->addQuery($query);
                }
                
                $multiQuery->run();
            }
        }
        else
        {
            if (count($rows) > 0)
            {
                $keys = array_keys($rows[0]);
                $escaped_keys = array();
                
                foreach ($keys as $key)
                {
                    $escaped_keys[] = mysqli_escape_string($this->m_mysqli, $key);
                }
                
                $quoted_keys = iRAP\CoreLibs\ArrayLib::wrapElements($escaped_keys, '`');
                
                foreach ($chunks as $row_set)
                {
                    $value_strings = array();
                    
                    foreach ($row_set as $row)
                    {
                        $values = array_values($row);

                        $escaped_values = array();
                        foreach ($values as $value)
                        {
                            if ($value !== null)
                            {
                                $escaped_values[] = mysqli_escape_string($this->m_mysqli, $value);
                            }
                            else
                            {
                                $escaped_values[] = null;
                            }
                        }

                        $quoted_escaped_values = iRAP\CoreLibs\ArrayLib::mysqliWrapValues($escaped_values);
                        $value_strings[] = " (" . implode(',', $quoted_escaped_values) . ")";
                    }
                    
                    $query = 
                        "INSERT INTO `" . $this->m_tableName . "` " .
                        "(" . implode(',', $quoted_keys) . ") " . 
                        "VALUES " . implode(',', $value_strings);
                    
                    $result = $this->query($query);
                    
                    if ($result === FALSE)
                    {
                        die("query failed:" . $query . PHP_EOL . $this->m_mysqli->error . PHP_EOL);
                    }
                }
            }
        }
    }
    
    
    /**
     * Deletes all rows that have the specified primary key values.
     * @param Array $keys - array list of values matching the primary keys of the rows we wish to remove.
     */
    public function deleteRows($keys)
    {
        $key_value_sets = array();
            
        foreach ($keys as $index => $set)
        {
            $quoted_set = \iRAP\CoreLibs\ArrayLib::wrapElements($set, "'");
            $key_value_sets[] = "(" . implode(',', $quoted_set) . ")";
        }
        
        $sql = 
            "DELETE FROM `" . $this->m_tableName . "` " .
            "WHERE (" . $this->getPrimaryKeyString() . ") " .
            "IN (" . implode(',', $key_value_sets) . ")";
        
        $this->query($sql);
    }
    
    
    /**
     * Fetches the sql statement that would be required to create this table from scratch.
     * e.g.
     * CREATE TABLE `table_name` (
     *  `column1` varchar(255) NOT NULL,
     *  `column2` decimal(6,4) NOT NULL,
     * PRIMARY KEY (`column1`)
     * ) ENGINE=InnoDB DEFAULT CHARSET=utf8 |
     * 
     * @param void
     * @return type
     */
    public function fetchCreateTableString()
    {
        $query = "SHOW CREATE TABLE `" . $this->m_tableName . "`";

        $result = $this->m_mysqli->query($query);
        $firstRow = $result->fetch_array();
        $creationString = $firstRow[1]; # the first column is the table name.
        return $creationString;
    }
    
    
    /**
     * Generates a hash for the entire table so we can quickly compare tables to see if they are 
     * already in syc.
     * Reference: http://stackoverflow.com/questions/3102972/mysql-detecting-changes-in-data-with-a-hash-function-over-a-part-of-table
     * @return String $hashValue - the md5 of the entire table of data.
     */
    public function fetchTableHash()
    {
        $wrappedColumnList = \iRAP\CoreLibs\ArrayLib::wrapElements($this->m_columns, "`");
        
        # do NOT use GROUP_CONCAT here since that has a very small default limit which results in not noticing 
        # differences on large tables
        $query = 
            "SELECT MD5( CONCAT_WS('#'," . implode(',', $wrappedColumnList) . ")) " .
            "AS `hash` " .
            "FROM `" . $this->m_tableName . "`";

        /* @var $result mysqli_result */
        $result = $this->m_mysqli->query($query);

        $string_of_row_hashes = "";
        while (($row = $result->fetch_assoc()) !== null)
        {
            $string_of_row_hashes .= $row['hash'];
        }

        $master_hash = hash("sha256", $string_of_row_hashes);

        $result->free();
        return $master_hash;
    }
    
    
    /**
     * Fetches the hashes for each row from the database. 
     * This will utilize quite a bit of the mysql hosts CPU.
     * @param type $keys
     * @return type
     */
    public function fetchRowHashes($keys)
    {
        $hashes = array();

        $key_sets = array_chunk($keys, 10000);
        
        foreach ($key_sets as $key_set)
        {
            $multi_query = new iRAP\MultiQuery\MultiQuery($this->m_mysqli);
            
            foreach ($key_set as $primaryKeyValue)
            {
                $primaryKeyValue     = \iRAP\CoreLibs\ArrayLib::wrapElements($primaryKeyValue, "'");
                $wrapped_column_list = \iRAP\CoreLibs\ArrayLib::wrapElements($this->m_columns, "`");

                $query = 
                    "SELECT MD5( CONCAT_WS('#'," . implode(',', $wrapped_column_list) . " ) ) " .
                    "AS `hash` " .
                    "FROM `" . $this->m_tableName . "` " .
                    "WHERE (" . $this->getPrimaryKeyString() . ") = (" . implode(",", $primaryKeyValue) . ")";

                $multi_query->addQuery($query);
            }

            $multi_query->run();

            foreach ($key_set as $index => $redundant)
            {
                $result_set = $multi_query->get_result($index);
                $row = $result_set[0]; #  there should only be one row
                $hashes[] = $row['hash'];
            }
        }
        
        return $hashes;
    }
    
    
    /**
     * Replace all the data_rows in the database by the primary keys specified in the index_values. The order of the
     * index_values
     * We delibereately delete all the rows before inserting the updates because we do not want to run into issues
     * with other unique keys etc. 
     * @param type $index_values
     * @param type $data_rows
     */
    public function replaceRows($index_values, $data_rows)
    {
        $multi_query = new iRAP\MultiQuery\MultiQuery($this->m_mysqli);
                
        $key_value_sets = array();
        
        # The primary key could itself be 
        foreach ($index_values as $index => $key_set)
        {
            $escaped_key_set = array();
            foreach ($key_set as $key)
            {
                $escaped_key_set[] = mysqli_escape_string($this->m_mysqli, $key);
            }
            
            $quoted_set = \iRAP\CoreLibs\ArrayLib::wrapElements($escaped_key_set, "'");
            $key_value_sets[] = "(" . implode(',', $quoted_set) . ")";
        }
        
        print "deleting rows that need replacing" . PHP_EOL;
        $delete_query = 
            "DELETE FROM `" . $this->m_tableName . "` " . 
            "WHERE (" . $this->getPrimaryKeyString() . ") " .
            "IN (" . implode(",", $key_value_sets) . ")";
        
        $deletion_result = $this->query($delete_query);
        
        print "inserting " . count($data_rows) . " replacement rows." . PHP_EOL;
        $this->insertRows($data_rows);
    }
    
    
    /**fetch_rows
     * Dynamically discovers the primary key for this table and sets this objects member variable accordingly.
     * @param void
     * @return void
     */
    private function fetchPrimaryKey()
    {
        $this->m_primaryKey = array();
        
        $query = "show index FROM `" . $this->m_tableName . "`";
        /*@var $result mysqli_result */
        $result = $this->m_mysqli->query($query);
        $this->m_primaryKey = null;
        
        while (($row = $result->fetch_assoc()) != null)
        {
            if ($row["Key_name"] === "PRIMARY")
            {
                $this->m_primaryKey[] = $row["Column_name"];
            }
        }

        if (count($this->m_primaryKey) == 0)
        {
            $this->m_primaryKey = null;
            print "WARNING: " . $this->m_tableName . " does not have a primary key!" . PHP_EOL;
        }

        $result->free();
    }
    
    
    /**
     * Fetches the names of the columns for this particular table.
     * @return type
     */
    private function fetchColumns()
    {        
        $sql = "SHOW COLUMNS FROM `" . $this->m_tableName . "`";
        $result = $this->m_mysqli->query($sql);
        
        $this->m_columns = array();
        
        while (($row = $result->fetch_array()) != null)
        {
            $this->m_columns[] = $row[0];
        }

        $result->free();
    }
    
    
    /**
     * Convert the primary key array into a string that can be used in queries.
     * e.g. array('id') would become: "(`id`)"
     * array(group, filter) would become "(`group`, `filter`)"
     * @return type
     */
    private function getPrimaryKeyString()
    {
        $wrapped_elements = \iRAP\CoreLibs\ArrayLib::wrapElements($this->m_primaryKey, '`');
        $csv = implode(',', $wrapped_elements);
        return $csv;
    }
    
    
    /**
     * Returns whether this table has a primary key or not.
     * @return boolean
     */
    public function hasPrimaryKey()
    {
        $result = true;
        
        if ($this->m_primaryKey === null)
        {
            $result = false;
        }
                
        return $result;
    }
    
    
    /**
     * Fetch the number of rows in the table
     * @return int
     */
    public function getNumRows()
    {
        $query = "SELECT COUNT(*) FROM `" . $this->m_tableName . "`";
        $result = $this->m_mysqli->query($query);
        /* @var $result \mysqli_result */
        $row = $result->fetch_array(MYSQLI_NUM);
        $result->free();
        return $row[0];
    }
    
    
    /**
     * Helper function that will execute queries on the database if we are not running a dry run
     * Hence all "write" queries should utilize this method, but "read" queries shouldnt if they still need to run
     * on a dry run
     * @param String $query - the query we want to send.
     * @return - the result from the query (or true if we are executing a "dry run")
     */
    private function query($query)
    {        
        $result = true;
        $result = $this->m_mysqli->query($query);
        return $result;
    }
    
    
    /**
     * Fetch the table in CSV form.
     * @param string $includeHeaders - optional - set to false to not include the headers.
     * @param array<string> - array of column names to order by (in ascending order)
     * @return string - the string representing the entire CSV file, includinge endlines.
     */
    public function fetchCSV($includeHeaders=true, $oderByArray = array())
    {        
        $resultString = "";
        $orderByString = "";
        
        if (count($oderByArray) > 0)
        {
            $orderByString = "ORDER BY ";
        
            foreach ($oderByArray as $column_name)
            {
                $orderByString .= $column_name  . " ASC, ";
            }
            
            # Remove the excess comma and space.
            $orderByString = substr($orderByString, 0, -2);
        }
        
        $query = "SELECT * FROM `" . $this->m_tableName . "` " . $orderByString;
        $result = $this->query($query);
        
        if ($result === FALSE)
        {
            throw new Exception("Failed to fetch content of table: " . $this->m_tableName);
        }
        
        $isFirstLine = true;
        
        while (($row = $result->fetch_assoc()) !== null)
        {
            if ($isFirstLine && $includeHeaders)
            {
                $titles = array_keys($row);
                
                foreach ($titles as $index => $title)
                {
                    $titles[$index] = '"' . $title . '"';
                }
                
                $resultString .= implode(",", $titles) . PHP_EOL;
                $isFirstLine = false;
            }
            
            foreach ($row as $column => $value)
            {
                if (!is_numeric($value))
                {
                    $row[$column] = '"' . $value . '"';
                }
            }
            
            $resultString .= implode(",", $row) . PHP_EOL;
        }
        
        return $resultString;
    }
}

