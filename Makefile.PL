use 5.008003;
use ExtUtils::MakeMaker;

print "Where do you want to install the mysql-backup script [/usr/bin]?";
my $bin_dir = <STDIN>;
chomp $bin_dir;
$bin_dir ||= '/usr/bin';

# See lib/ExtUtils/MakeMaker.pm for details of how to influence
# the contents of the Makefile that is written.
WriteMakefile(
    NAME              => 'DBA::Backup::MySQL',
    VERSION_FROM      => 'lib/DBA/Backup.pm', # finds $VERSION
    PREREQ_PM         => {}, # e.g., Module::Name => 1.1
    ($] >= 5.005 ?     ## Add these new keywords supported since 5.005
      (ABSTRACT_FROM  => 'lib/DBA/Backup.pm', # retrieve abstract from module
       AUTHOR         => 'Sean Quinlan <sean@quinlan.org>') : ()),
    INSTALLSCRIPT     => $bin_dir,
);

print <<CONF;


################################################################################
###
### You will need to edit the configuration file (etc/mysql-backup.conf) with
### your local settings and copy to a location of your choice. The included
### backup script will look for /etc/mysql-backup.conf by default, but it can
### be installed anywhere you want and the location can be provided to the
### backup script as an argument. Since you will need to have a valid username
### and password in the conf file, make sure it is readbale only by root. There
### is also a sample user creation SQL script to generate a user with the
### minimal permissions to make backups.
###
### Thanks!!!
###
################################################################################

CONF
