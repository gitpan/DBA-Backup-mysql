-- SQL script to create account for database backup
-- this user can log in only from localhost
-- this user has select privilege on every database
-- this user has privilege to flush logs
-- this user has privilege to see all process info (and kill mysql processes)

-- create the account for localhost only
-- execute the mysql password function on the password, fill in result on next line
GRANT USAGE ON *.* TO 'backup'@'localhost' IDENTIFIED BY PASSWORD 'encrypted secret' ;

-- grant the select privileges
GRANT SELECT ON *.* TO 'backup'@'localhost' ;

-- grant the process privilege
GRANT PROCESS ON *.* TO 'backup'@'localhost' ; 

-- grant the lock tables privilege
GRANT LOCK TABLES ON *.* TO 'backup'@'localhost' ;

-- grant the flush privilege
GRANT RELOAD ON *.* TO 'backup'@'localhost' ;

-- flush after every change
FLUSH PRIVILEGES; 

-- verify changes
SHOW GRANTS for 'backup'@'localhost';



