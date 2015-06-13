<?php

/* 
 * Singelton for providing the connection details for the VidaDB objects. This prevents the user from having to 
 * manipulate the package's code.
 */

namespace iRAP\Mysqli;

class ConnectionHandler
{
    private static $s_connections = array(); # \mysqli object stored by the single instance
    private static $s_connectionDetails = array(); # array of connection detail objects
    private static $s_lastQueryTime = array(); # array of last time the database was queried.
    
    /**
     * Set the connection details for this class. This must be run before being able to use any
     * of the VidaDb classes. 
     * @param string $host - the host that has the database
     * @param string $user - the user to connect with
     * @param string $password - the password to connect with
     * @param string $db_name - the name of the db, such as "vida"
     * @param int $port - optionally specify the port if it is not the default mysql port
     */
    public static function addConnectionDetails($connectionName, $host, $user, $password, $db_name, $port=3306)
    {
        $connectionDetails = new \stdClass();
        $connectionDetails->host = $host;
        $connectionDetails->user = $user;
        $connectionDetails->password = $password;
        $connectionDetails->db_name = $db_name;
        $connectionDetails->port = $port;
        
        self::$s_connectionDetails[$connectionName] = $connectionDetails;
    }
    
    
    /**
     * Closes a connection and reconnects to it.
     * @param string $connectionName - the name of the connection we wish to reconnect
     * @return MySqli $connectionName
     */
    public static function reconnect($connectionName=null)
    {
        # create connection will close an existing connection anyway!
        self::createConnection($connectionName);
    }
    
    
    /**
     * Run a mysql query.
     * If this class hasn't queried that connection in a while, then it will check the connection 
     * is open and reconnect if not.
     * @param string $query - the query to run
     * @param string $errorMessage - optional error message to put into an exception if query fails
     * @param string $connectionName - optional the name of the connection if you have more than one
     */
    public static function query($query, $errorMessage='', $connectionName=null)
    {
        $connectionName = self::connectionNameGuard($connectionName);
        
        if 
        (
            !isset(self::$s_lastQueryTime[$connectionName]) ||
            (time() - self::$s_lastQueryTime[$connectionName]) > 60
        )
        {
            self::checkConnection($connectionName);
        }
        
        
        $connection = self::getConnection($connectionName);
        
        /* @var $connection \mysqli */
        $errorFunc = function($errorMessage, $query) use ($connection)
        {
            $messageArray = array(
                "message" => $errorMessage, 
                "error "  => $connection->error,
                "query"   => $query
            );
            
            $errorMessage = json_encode($messageArray, JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES);
            
            throw new Exception($errorMessage);
        };
        
        
        if ($errorMessage !== '')
        {
            $result = $connection->query($query) or $errorFunc($errorMessage, $query);
        }
        else
        {
            $result = $connection->query($query);
        }
        
        return $result;
    }
    
    
    /**
     * Closes off a single connection by name.
     * @param connectionName - the name of the connection we wish to close off.
     * @return void - closes connections in this classes member variables.
     */
    public static function closeAll()
    {
        foreach (self::$s_connections as $connection)
        {
            self::$connection->close();
        }
    }
    
    
    /**
     * Check that a connection is still connected. This is only necessary when there is expected
     * to be a long period between mysql queries so that the connection may have timed out.
     * This function will auto-reconnect if it has timed out without the dev needing to do anything
     * @param String $connectionName - the name of the connection that you wish to reconnect to
     *                                 if it has timed out.
     */
    public static function checkConnection($connectionName=null)
    {
        $connectionName = self::connectionNameGuard($connectionName);
        
        if (!mysqli_ping(self::$s_connection))
        {
            self::reconnect();
        }
    }
    
    
    /**
     * Closes off a single connection by name.
     * @param connectionName - the name of the connection we wish to close off.
     * @return void - closes connections in this classes member variables.
     */
    public static function closeConnection($connectionName=null)
    {
        $connectionName = self::connectionNameGuard($connectionName);
        
        if (isset(self::$s_connections[$connectionName]))
        {
            $connection = self::$s_connections[$connectionName];
            $connection->close();
            unset(self::$s_connections[$connectionName]);
        }
    }
    
    
    /**
     * Accessor to the mysqli connection
     * @return \mysqli
     */
    public static function getConnection($connectionName=null)
    {
        $connection = null;
        
        $connectionName = self::connectionNameGuard($connectionName);
        
        # Connect to the database if we have not created a connection yet!
        if (!isset(self::$s_connections[$connectionName]))
        {
            self::createConnection($connectionName);
        }
        
        $connection = self::$s_connections[$connectionName];
        
        return $connection;
    }
    
    
    /**
     * Check the connection name is set. If we have only one connection then assume
     * that they want that connection.
     * @param string $connectionName - the name of the connection.
     * @return type
     * @throws Exception
     */
    private static function connectionNameGuard($connectionName=null)
    {
        if ($connectionName === null && count(self::$s_connectionDetails) > 0)
        {
            throw new Exception('You need to specify which connection to close!');
        }
        
        if ($connectionName === null && count(self::$s_connectionDetails) == 1)
        {
            $connectionNames = array_keys(self::$s_connections);
            $connectionName = $connectionNames[0];
        }
        
        if (!isset(self::$s_connectionDetails[$connectionName]))
        {
            throw new \Exception('Undefined connection [' . $connectionName . ']');
        }
        
        return $connectionName;
    }
    
    
    /**
     * Creates a brand new mysqli connection. If one already exists, it will close it and 
     * create another.
     * @return \mysqli
     */
    private static function createConnection($connectionName=null)
    {
        $connectionName = self::connectionNameGuard($connectionName);
        
        # close existing connection if it already exists.
        closeConnection($connectionName);
        
        $connectionDetails = self::$s_connectionDetails[$connectionName];
        
        $connection = new \mysqli($connectionDetails->host, 
                                  $connectionDetails->user, 
                                  $connectionDetails->password, 
                                  $connectionDetails->db_name, 
                                  $connectionDetails->port);
        
        $connection->set_charset('UTF8');
        
        self::$s_connections[$connectionName] = $connection;
        self::$s_lastQueryTime[$connectionName] = time();
        return $connection;
    }
}