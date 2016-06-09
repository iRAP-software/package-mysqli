<?php

/* 
 * File to test the TableConnection class.
 */


class TableConnectionTest
{
    public static function run()
    {
        $dbConn = new \iRAP\Mysqli\DatabaseConnection(
            TEST_DB_HOST,
            TEST_DB_USER, 
            TEST_DB_PASSWORD, 
            TEST_DB_NAME
        );

        $testTableName = "test_table";
        $dbConn->query("DROP TABLE `" . $testTableName . "`");

        $createTableQuery = 
            "CREATE TABLE `" . $testTableName . "` (
            `id` int(11) NOT NULL AUTO_INCREMENT,
            `name` varchar(255) NOT NULL,
            `redirect_url` text NOT NULL,
            `secret` varchar(30) NOT NULL,
            PRIMARY KEY (`id`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8;";

        $dbConn->query($createTableQuery);

        $insertData = array(
            array('id' => 1, 'name' => "site1", "redirect_url" => "www.site1.com", "secret" => "dfdfeerw"),
            array('id' => 2, 'name' => "site2", "redirect_url" => "www.site2.com", "secret2" => "adfdadfd"),
            array('id' => 3, 'name' => "site3", "redirect_url" => "www.site3.com", "secret3" => "derhrtre"),
        );

        $insertQuery = "INSERT INTO `" . $testTableName . "` (`id`, `name`, `redirect_url`, `secret`) VALUES";

        foreach ($insertData as $row)
        {
            $insertQuery .= "('" . implode("', '", array_values($row)) . "'), ";
        }

        $insertQuery = substr($insertQuery, 0, (strlen($insertQuery) - 2)); # remove last comma.
        $dbConn->query($insertQuery) or die($dbConn->getMysqli()->error);
        $tableConnection = new \iRAP\Mysqli\TableConnection($dbConn, $testTableName);
        $resultString = $tableConnection->fetchCSV();

        $expectedResult = 
            '"id","name","redirect_url","secret"'  . PHP_EOL .
            '1,"site1","www.site1.com","dfdfeerw"' . PHP_EOL .
            '2,"site2","www.site2.com","adfdadfd"' . PHP_EOL .
            '3,"site3","www.site3.com","derhrtre"' . PHP_EOL;

        if ($resultString !== $expectedResult)
        {
            print "TableConnection: FAILED" . PHP_EOL;
        }
        else
        {
            print "TableConnection: PASSED" . PHP_EOL;
        }
    }
}




