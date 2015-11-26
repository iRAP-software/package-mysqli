<?php

namespace iRAP\Mysqli;

class DatabaseConnection
{
    private $m_mysqli;
    private $m_tableList = null;
    
    
    /**
     * Construct a connection object. 
     * @param string $host - the host that the database is on, such as localhost
     * @param string $user - the user to connect with
     * @param string $password - the password that corresponds to the user
     * @param string $database - the name of the database on the host
     * @param int $port - optionally specify the port if it is not the default 3306
     */
    public function __construct($host, $user, $password, $database, $port=3306)
    {   
        $this->m_mysqli = new \mysqli($host, $user, $password, $database, $port) 
            or die("Failed to connect to $host database");
    }
    
    
    
    /**
     * Fetch the list of tables that exist in the database. This will cache the result so that subsequent calls will
     * return almost immediately.
     * @return Array<String> - list of table names
     */
    public function fetchTableNames()
    {
        if ($this->m_tableList === null)
        {
            $tables = array();
        
            $query = "SHOW TABLES";
            $result = $this->m_mysqli->query($query);

            while (($row = $result->fetch_array()) != null)
            {
                $tables[] = $row[0];
            }
            
            $this->m_tableList = $tables;
        }
        
        return $this->m_tableList;
    }
    
    
    /**
     * Executes a passed in query if we are not using a DRY_RUN.
     * This should be used for all "write" queries but not any "read" queries that need to actually execute on a dry 
     * run
     * It is better to use this object's other methods wherever possible.
     * @param string $query - the query to execute.
     * @return \mysqli_result
     */
    public function query($query)
    {
        global $settings;
        
        $result = true;
        
        if ($settings['LOG_QUERIES'])
        {
            $line = $query . PHP_EOL;
            file_put_contents($settings['LOG_QUERY_FILE'], $line, FILE_APPEND);
        }
        
        if (!$settings['DRY_RUN'])
        {
            $result = $this->m_mysqli->query($query);
        }
        
        return $result;
    }
    
    
    /**
     * Drop a table in the database by name.
     * @param String $tableName - the name of the table to drop.
     * @return void - throws exception if failed.
     */
    public function dropTable($tableName)
    {
        print "Dropping table " . $tableName . PHP_EOL;
        $query = "DROP TABLE IF EXISTS `" . $tableName . "`";
        $this->query($query);
    }
    
    
    # Accessors
    public function getMysqli() { return $this->m_mysqli; }
}

