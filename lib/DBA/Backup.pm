package MySQL::Backup;


=head1 NAME

mysql-backup

=head1 SYNOPSIS

Please see MySQL::Backup thread in module-authors mailing list at perl.org
<module-authors@perl.org>. I just copied everything from my MySQL::Backup
project into this namespace as a result of that discussion. I'll be breaking
out the backup management and mysql specific parts and getting this dist passing
basic tests again ASAP.

  shell% mysql-backup /path/to/mysql-backup.conf

=cut

use 5.008003;
use strict;
use warnings;

use DBI;            # needed for database connections 
use Sys::Hostname;  # provides hostname()
use File::Copy qw/move/;
use File::Path qw/rmtree/;
use Mail::Sendmail; # for sending mail notifications


our $VERSION = '0.8';

umask(0117); # prevent this script from granting any privilege to all (other users)


use warnings;
use strict;
use YAML qw(LoadFile); # config file processing

our $AUTOLOAD;

# we'll set up an autoload method to get and set values for
# each section in config file
our @sections = ();

# all error and notification messages are aggregate here before being written
# to the log file
our @LOG; 

=head1 new() constructor for Backup::Config object

    Gets all config information from the config file and also
    gets some data and values from the database itself. Then 
    stores all this information as attributes in 
    Backup::Config object. All attributes are accessed by 
    method $obj->section(attr_name) where attr_name should be replaced
    by the name of the attribute which value is requested.
    
    To set the value, call $obj->section->attr_name($val)
    All accessors and mutators are created by AUTOLOAD.

=cut

sub new {
	my $class   = shift;
	my %params = ref($_[0]) eq 'HASH' ? %{$_[0]} : @_;
	
	# exits with usage statement if the config file is not valid
	_is_config_file_valid($params{CONF_FILE}) or usage($params{CONF_FILE});
	
	# Read the YAML formatted configuration file
	my $HR_conf = LoadFile($params{CONF_FILE})
		or die("Problem reading conf file $params{CONF_FILE}");
	
	# now lets modify for any command line arguments
	$HR_conf->{LOG_FILE} = $params{logfile} if $params{logfile};
	my $cur_day = substr(localtime,0,3);
	$HR_conf->{backup}{days} = $cur_day if $params{backup};
	if ($params{add_databases}) {
		my $AR_dbs = $HR_conf->{backup}{databases};
		foreach my $db (split(/ ?, ?/,$params{add_databases})) {
			push(@{$AR_dbs},$db) unless grep (/$db/, @{$AR_dbs});
		} # for each db to add
	} # if backing up additional databases 
	
	
	# record the sections found in the file
	@sections = keys %{$HR_conf};
	
	# Stores the name of the current config file in the object
	$HR_conf->{backup_params}{CONF_FILE} = $params{CONF_FILE};
	$HR_conf->{db_connect}{HOSTNAME}  = Sys::Hostname::hostname();
	
	
	# Parse the db_connect group for user id and password
	my $dbn = 'DBI:mysql:mysql:mysql_socket='
		. $HR_conf->{db_connect}{SOCKET};
	
	# Database connection parameters
	$HR_conf->{DBI} = DBI->connect($dbn, $HR_conf->{db_connect}{USER},
		$HR_conf->{db_connect}{PASSWORD});
	die("Can't connect to the database $dbn\n") unless ref $HR_conf->{DBI};
	
	# since cron jobs frequently have missing or incomplete path information
	# we'll require this to be done by config
#	my $exe =  _get_program_path('mysqladmin');
#	die "No path to mysqladmin found";
#	$self->{bin_dir}{mysqladmin} = $exe;
	
	return bless $HR_conf, $class;
} # end new()


=head1 _get_program_path()

	Gets a name of a program and returns the full path to it.
	If the path contains anything than slashes, alhpanumeric charactes 
	and underscores and dashes undef is returned.

=cut

sub _get_program_path {
	my $program = shift;
	my $path = '';#File::Which::which($program);
	#my $path = `which $program`;
	
	return '' unless $path;
	
	# remove all spaces
	$path =~ /\s+/g;
	
	# reject programs which have funky characters in them
	return '' unless $path =~ /^[\w_\/-]+$/;  
	
	return $path;
} # end _get_program_path()


=head1 usage()

    Prints an usage message for the program on the screen and then exits.

=cut 

sub usage {
	my $file = shift;
	die "Usage: $0 /path/backup.cnf\n"
		. "Please make sure you specified a config file with proper format."
		. "$file provided.\n"; 
}


sub _is_config_file_valid {
	my $file = shift;
	return 0 unless $file;
	
	# if file name does not start with ./ or /, then append ./
#	unless ($file =~ m{^[./]} ) {
#		$_[0] = "./$_[0]";
#	}
	
	unless (-f $file && -r $file) {
		return 0;
	} # unless file exists and is reabable
	
	return 1;
}

sub AUTOLOAD {
	my $self = shift;
	my $key = shift;
	
#	warn "AUTOLOADing $AUTOLOAD, $key";
	
	my $get = my $set = $AUTOLOAD;
	$get =~ s/.+:://; # strip any package information
	$set =~ s/::[^:]+$//;
	$set =~ s/.+:://;
	
	# see if method called is section->attr_name() used
	# to set a value
	my @set_val = grep($_ eq $set, @sections);
	if (@set_val) {
		$self->{$set_val[0]}{$1} = $key;
		return 1;
	} # if setting value
	
	if ($get eq 'DBI') { return $self->{DBI} }
	
	unless (exists $self->{$get}) {
#		warn "$get does not appear to be valid";
		return 0;
	} # unless valid section
	
	# this will return the value of the key requested
	return $self->{$get}{$key} || '';
} # end AUTOLOAD()


##
## The above section sets up and handles configuration parsing
## The section below actually manages the backups
##


=head1 run()

    This is where most of the work in the program is done.
    It logs some messages to the log file and invokes the subroutines
    for database backup and log backup and rotation.

=cut

sub run {
	my $self = shift;
	
	my $time = localtime();
	my $host = $self->db_connect('HOSTNAME');
	my $log_dir = $self->backup_params('LOG_DIR');
	my $dump_dir = $self->backup_params('DUMP_DIR');
	my $dump_copies = $self->backup_params('DUMP_COPIES');
	my $conf_file = $self->backup_params('CONF_FILE');
	
#	warn "Starting log";
	
	# Greeting message
	push @LOG, <<LOG;
	
*** MySQL backup script ($0) started [$time] ***
Host and mysql server: $host
Log dir: $log_dir
Dump dir: $dump_dir
Dumps to keep: $dump_copies
Backup config file: $conf_file
	
*Tidying up dump dirs*
LOG
	
#	warn "_tidy_dump_dirs";
	# clean up dump dirs
	$self->_tidy_dump_dirs();
	
	# check the disk space on the dump directory 
	# for now, use the unix df command, later use perl equivalent
	my $disk_usage = `df '$dump_dir'`; 
	push @LOG, "\n\n*Disk space report for $dump_dir*\n$disk_usage";
	
	# check the list of currently running mysql queries
	push @LOG, "\n\n*Current processlist*\n";
#	warn "_get_process_list";
	push @LOG, $self->_get_process_list();
	
	# rotate the logs.  most text log files need to be renamed manually
	# before the "flush logs" command is issued to mysqld
	# we rotate logs daily (well, every time script is run)
	push @LOG, "\n\n*Rotating logs*\n";
#	warn "_rotate_general_query_log";
	$self->_rotate_general_query_log();
#	warn "_rotate_slow_query_log";
	$self->_rotate_slow_query_log();
#	warn "_cycle_bin_logs";
	$self->_cycle_bin_logs();
#	warn "_rotate_error_log";
	$self->_rotate_error_log();
	
	# Backup the databases only if today is the day which is 
	# specified in the config file
	my $cur_day = substr(localtime,0,3);
	my @backup_days = split(/ ?, ?/,$self->backup('days'));
	
	if (grep(/$cur_day/i, @backup_days)) {
		push @LOG, "\n\n*Starting dumps*\n";
#		warn '_backup_databases';
		my $b_errs = $self->_backup_databases();
		
		# if there were no problems backing up databases, rotate the dump dirs
		if (not $b_errs) {
			push @LOG, "\n\n*Rotating dump dirs*\n";
#			warn '_rotate_dump_dirs';
			$self->_rotate_dump_dirs();
		} # if no backup errors
	} # if today is a database backup day
	
	# Goodbye message
	$time = localtime();
	push @LOG, "\n\n*** MySQL backup script ($0) finished [$time] ***\n\n";
	
} # end run()


=head1 _get_process_list()

	Returns a list of all mysql processes running currently on the server.

	Gets the processlist from dbms and print it to the LOG the fields are
	as follows:
	Id  User Host db Command Time State Info

	The assumption is that these fields will not change. It's hard to make
	a dynamic script because LISTFIELDS works only on tables, and retrieval
	to a hash does not preserve the order of the fields.

=cut 

sub _get_process_list {
	my $self = shift;
	
	my $dbh = $self->DBI;
	my $proc_ref = $dbh->selectall_arrayref('show processlist');
	my $mesg = "";
	
	# extract the summary
	foreach my $row_ref (@{$proc_ref}) {
		foreach my $field (@{$row_ref}) {
			$field = 'NULL' if not defined $field;
			$mesg .= "$field ";
		} # for each field
		$mesg .= "\n"; 
	} # for each process
	
	return $mesg;
} # end _get_process_list()


=head1 log_messages

	Logs all messages accumulated so far to a log file 
	which name is specified in the config variable log_file. 

=cut 

sub log_messages {
	my $self = shift;
	
	my $logfile = $self->backup_params('LOG_FILE');
	
	unless (open(FILE, '>>' . $logfile)) {
		push(@LOG, "Cannot open log file. Exiting prematurely");
		$self->send_email_notification();
		exit 1;
	}
	print FILE @LOG;
	close FILE or warn "$!";
} # end log_messages()


=head1 _test_create_dirs

	Test for the existence and writeability of specified directories.
	If the directories do not exist, attempt to create them.  If unable
	to create writeable directories, fail with error.

=cut

sub _test_create_dirs {
	my $self = shift;
	
	# check the existence and proper permissions on all dirs
	foreach my $dir (@_) {
		# if it doesn't exist, create it
		unless (-d $dir) {
			push @LOG, "Directory $dir does not exist, creating it...\n";
			my $mask = umask;
			umask(0007);
			unless (mkdir($dir) and -d $dir) {
				$self->error("Cannot create $dir.\n");
			} # unless dir created
			umask($mask);
		} # if directory doesn't exist
		# check that we can write to it
		unless (-w $dir) {
			$self->error("Directory $dir is not writable by the user running $0\n");
		} # if directory isn't writable
	} # foreach directory to be tested

} # end _test_create_dirs()


#################### ROTATE AND FLUSH LOGS PRIOR TO DATABASE DUMPS ###########

sub _rotate_generic_log {
	my $self = shift;
	
	my $logname = shift;
	my $conf_rotate = shift;
	my $log_file = shift;
	my $max_log_size = shift;
	my $max_log_count = shift;

	push @LOG, "\n";

	# test whether user wants us to rotate
	unless ( $conf_rotate =~ /^yes$/i ) {
		push @LOG, "$logname log is configured not to be rotated\n";
		return;
	} # don't rotate unless yes

	# test if file exists
	unless (-f $log_file) {
		push @LOG, "$logname log doesn't exist\n";
		return;
	} # only rotate if it exists
	push @LOG, "$logname log is $log_file\n";

	# test if file is larger than max log size
	unless (-s $log_file > ($max_log_size*1024*1024) ) {
		push @LOG, "$logname log did not need rotating\n";
		return;
	} # rotate log if larger than max log size

	# rename all of the old logs, keep only as many of them as set by the
	# config parameter
	for (my $i = $max_log_count - 2; $i >= 0; $i--) {
		my $j = $i + 1; 
		if (-f "$log_file.$i") {
			push @LOG, "Renaming $log_file.$i to $log_file.$j\n"; 
			rename("$log_file.$i", "$log_file.$j");
		} # if log at iteration exists, move down
	} # for each possible log iteration
	
	# rename the current log to .0
	rename($log_file, "$log_file.0");
	
	# done
	push @LOG, "$logname log rotated\n";

} # end _rotate_generic_log()

sub _rotate_general_query_log {
	my $self = shift;
	
	$self->_rotate_generic_log("General query",
			$self->backup_params('ROTATE_GEN_QUERY_LOGS'),
			$self->backup_params('LOG_DIR') . '/'
			. $self->db_connect('HOSTNAME') . '.log',
			$self->backup_params('MAX_GEN_LOG_SIZE'),
			$self->backup_params('MAX_GEN_LOG_FILES'));
} # end _rotate_general_query_log()

sub _rotate_slow_query_log {
	my $self = shift;
	
	$self->_rotate_generic_log("Slow query",
			$self->backup_params('ROTATE_SLOW_QUERY_LOGS'),
			$self->backup_params('LOG_DIR') . '/'
			. $self->db_connect('HOSTNAME') . '.log',
			$self->backup_params('MAX_SLOW_LOG_SIZE'),
			$self->backup_params('MAX_SLOW_LOG_FILES'));
} # end _rotate_slow_query_log()

=head1 _rotate_error_log()

	The mysql error logs don't operate the same way as the other logs.
	As of mysql 4.0.10, every flush-logs command will cause the error log
	to rotate to a file with an "-old" suffix attached.  This is
	regardless of the file's size.  Mysql shutdown/startup will *not*
	rotate the error log to the -old file.  Any previous -old file
	is deleted.

	This function attempts to restore some sanity to how mysql treats
	the error log.  Call this function after the flush-logs command.
	We will take new -old file and append it to the end of our own file,
	(different name) and delete the -old file.  We'll then call the usual
	_rotate_generic_log function on it.

=cut 

sub _rotate_error_log {
	my $self = shift;
	
	my $log_dir = $self->backup_params('LOG_DIR');
	my $hostname = $self->db_connect('HOSTNAME');
	my $log_in  = $log_dir . '/' . $hostname . '.err-old';
	my $log_out = $log_dir . '/' . $hostname . '-error.log';
	push @LOG, "\n";
	
	# test if file exists
	unless (-f $log_in) {
		push @LOG, "mysqld old error log ($log_in) doesn't exist\n";
		return;
	} # if old err log doesn't exist
	push @LOG, "mysqld old error log is $log_in\n";
	push @LOG, "... merging into cumulative error log $log_out\n";
	
	# merge mysql droppings into our own log file
	open(INFILE,$log_in) or die "Problem reading $log_in: $!";
	open(OUTFILE,">>$log_out") or die "Problem appending $log_out: $!";
	while (<INFILE>) { print OUTFILE $_; }
	close OUTFILE or warn "$!";
	close INFILE or warn "$!";
	unlink($log_in);
	
	# perform usual log rotation on merged file
	_rotate_generic_log("Cumulative error",
			$self->backup_params('ROTATE_ERROR_LOGS'),
			$log_dir . '/' . $hostname . '.log',
			$self->backup_params('MAX_ERROR_LOG_SIZE'),
			$self->backup_params('MAX_ERROR_LOG_FILES'));
} # end _rotate_error_logs()


=head1 _cycle_bin_logs()

	Issues command to mysqld to finish writing to the current binary
	update log and start writing to a new one.  We then push all of
	the bin-logs (except for the newest one) into [dump_dir]/00/.

	The flush logs command causes mysqld to close the old (already renamed)
	general query and slow query logs and reopen the logs of the usual
	file name.  It also causes mysqld to flush the binary update log and
	begin writing to a new binlog file.  It does not affect the error 
	log, only a restart of mysqld will start a new error log.

	The flush hosts command will clean up the hosts cache.

=cut 

sub _cycle_bin_logs {
	my $self = shift;
	
	my ($hostname) = $self->db_connect('HOSTNAME') =~ m/^([^\.]+)/;
	my $data_dir = $self->backup_params('LOG_DIR');	
	my $dump_dir = $self->backup_params('DUMP_DIR') . '/00';
	
	# test whether user wants us to cycle
	my $cur_day = substr(localtime,0,3);
	my @backup_days = split(/ ?, ?/,$self->backup('days'));
	unless (($self->backup_params('CYCLE_BIN_LOGS_DAILY') =~ /^yes$/i ) 
			or (grep(/$cur_day/i, @backup_days))) {
		push @LOG, "Binary log is not configured to be cycled today";
#		warn 'Binary log is not configured to be cycled today';
		return;
	} # if bin logs only backed up on full dump days and that aint today
	
	# get a list of all existing binlog files to back up
	opendir(DIR, $data_dir) 
		or $self->error("Cannot open directory where bin log files reside\n");
	my @binlog_files = grep { /$hostname\-bin\.\d+/ } readdir(DIR);
	closedir(DIR);
#	warn "Found @binlog_files in $data_dir";
	
	# prepare the flush command
	my $mysqladmin_path = $self->bin_dir('mysqladmin');
	my $username = $self->db_connect('USER');
	my $password = $self->db_connect('PASSWORD');
	my $db_host = $self->db_connect('RDBMS_HOST');
	my $socket = $self->db_connect('SOCKET');
	my $cmd = "$mysqladmin_path -u$username -p$password --host=$db_host "
		. "--socket=$socket "; 
	$cmd .= join(' ', map { "--$_" } @{$self->mysqladmin('options')});
	$cmd .= " flush-logs flush-hosts";
	
#	warn "flushing logs with $cmd";
	# execute the flush command
	if (-x $mysqladmin_path) {
		my $rc = system($cmd);
		$self->error("\nError occured while trying to flush-log mysqld\n") if $rc;
		push @LOG, "\nmysql flush-logs and flush-hosts commands were issued\n";
		push @LOG, "... binary update log was cycled\n";
	} # if we're allowed to execute
	else {
		$self->error("mysql flush-logs failed!!! mysqladmin is not executable\n");
	} # else bitch
	
	# back up the binary update logs
#	warn 'backing up bin log';
	push @LOG, "\nBacking up binary update logs\n";
	push @LOG, "Moving binlogs from $data_dir/ to $dump_dir/ ...\n";
	foreach my $file (@binlog_files) {
		my $rc = File::Copy::move("$data_dir/$file", "$dump_dir/$file");
		if ($rc) {
			push(@LOG, "... moved $file\n");	
		} # if move succesful
		else {
			$self->error("Can't move the binary log file $file - $!($rc)\n");
		} # else die
	} # foreach bin log
	push @LOG, "Backed up " . int(@binlog_files) . " binary update logs\n";
	
} # end _cycle_bin_logs()


=head1 _backup_databases()

	Backup all databases on the server DBMS which are mentioned 
	explicitly or as a pattern in the [included-databases] section 
	in the config file.

	This function will dump all specified databases to .sql.gz files
	in the directory [dump_dir]/new/.  If there were no errors during
	backup, _rotate_dump_dirs will then rename it [dump_dir]/00/.

	If this function encounters errors during backup, the partial dumps 
	to [dump_dir]/new/ will remain until the next time this function is
	executed.  At that time, the contents of [dump_dir]/new/ will be
	destroyed and new dumps will be placed there.

	At no time are binary update logs ever placed in [dump_dir]/new/.

	Return with the number of errors encountered during backup.

=cut

sub _backup_databases {
	my $self = shift;
	
	my $dump_dir = $self->backup_params('DUMP_DIR') . '/new';
	my $backup_errors = 0;
	
	# create the new/ dump_dir, but delete it if it already exists
	if (-d $dump_dir) {
		push @LOG, "Partial/failed dumps in $dump_dir exist, deleting...\n";
		eval { File::Path::rmtree($dump_dir) };
		$self->error("Cannot delete $dump_dir - $@\n") if ($@);
		$self->error("$dump_dir deleted, but still exists!\n") if (-d $dump_dir);
	} # if directory exists
#	warn '_test_create_dirs';
	$self->_test_create_dirs($dump_dir);
	
	# dump a .sql.gz file for each database into the dump_dir
	foreach my $database ( @{$self->backup('databases')} ) {
#		warn "Backing up $database";
		
		# get the date, parsed into its parts
		my ($sec, $min, $hour, $mday, $mon, $year, $wday, $yday, $isdst) 
			= localtime();
		$year += 1900; $mon += 1;
		
		# build part of the output file name from the date parts
		my $date_spec = $year . sprintf("%02d%02d%02d%02d", $mon, $mday, 
			$hour, $min);
		my $time_stamp = sprintf("%04d-%02d-%02d %02d:%02d:%02d", 
			$year,$mon,$mday,$hour,$min,$sec);
		my $db_host = $self->db_connect('RDBMS_HOST');
		my $hostname = $self->db_connect('HOSTNAME');
		(my $short_hostname) = $hostname =~ s/^([^\.]+).*$/$1/;
		my $dump_file = $date_spec .'_'. $short_hostname .'_'. $database 
			. '.sql.gz';
		
		push @LOG, "[$time_stamp] Dumping $database to $dump_file\n";
		
		# build the dump command line in steps
		my $gzip = $self->bin_dir('gzip');
		my $mysqldump = $self->bin_dir('mysqldump');
		my $username = $self->db_connect('USER');
		my $password = $self->db_connect('PASSWORD');
		my $socket = $self->db_connect('SOCKET');
		
		my $cmd = "$mysqldump -u$username -p$password --host=$db_host "
			. "--socket=$socket"; 
		$cmd .= join(' ',
			map { "--$_" } @{$self->mysqldump('options')});
		$cmd .= "$database | $gzip -9 > $dump_dir/$dump_file";
		
		# make sure that the database backup went fine
#		warn "Dumping with $cmd";
		my $rc = system($cmd);
		if ($rc) {
			$cmd =~ s/ -p$password / -pPASSWORD_HIDDEN /;
			push(@LOG, "An error occured while backing up database $database - $rc - '$cmd'\n");
			$backup_errors++;
		} # if there was an error executing command
		
	} # foreach $database
	
	# print timestamp one more time when it's all done
	my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime;
	$year += 1900; $mon += 1;
	my $time_stamp = sprintf("%04d-%02d-%02d %02d:%02d:%02d", 
				 $year,$mon,$mday,$hour,$min,$sec);
	push @LOG, "[$time_stamp] Compressed dumps to $dump_dir/ "
	. "completed with $backup_errors errors\n";
	
	return $backup_errors;
} # end _backup_databases()


=head1 _rotate_dump_dirs()

	The dump directories contain output from both the full, weekly mysql
	dump as well as the incremental binary update logs that follow the
	dump (possibly multiple binlogs per day).  Rotate these directory 
	names to conform to convention:

	  [dump_dir]/00/  - most recent dump
	  [dump_dir]/01/   - next most recent
	  ...
	  [dump_dir]/_NN/	- oldest

	Where N is [dump_copies] - 1 (in the config file).  [dump_dir]/new/
	is a temporary directory created from _backup_databases.  This will
	be renamed 00/, 00/ will be renamed 01/, and so on.

=cut

sub _rotate_dump_dirs {
	my $self = shift;
	
	my $dump_root = $self->backup_params('DUMP_DIR');
	my $max_old   = $self->backup_params('DUMP_COPIES') -1;
	$max_old = 0 if $max_old < 0;

	# grab list of files/dirs within dump_root - @dump_root_files
	# create hash of dirs we care about w/in dump_root - %dump_root_hash
	opendir(DIR, $dump_root) 
		or $self->error("Cannot get listing of files in $dump_root\n");
	my @dump_dirs = grep {-d "$dump_root/$_"} readdir(DIR);
	closedir(DIR);
	my %dump_root_hash = ();
	foreach my $dir (@dump_dirs) {
		$dump_root_hash{$dir} = 1 if $dir =~ /^\d+$/;
		$dump_root_hash{$dir} = 1 if $dir =~ /^00$/;
		$dump_root_hash{$dir} = 1 if $dir =~ /^new$/;
	} # for each dump root file

	# prepare instructions on how to rename directories, and in what order
	# do not rename a dir unless it "needs" to be renamed
	
	# this seems wicked kludgy, but I want to understand reasoning before I
	# throw away or improve - spq
	my @dir_order;
	my %dir_map;
	if (exists $dump_root_hash{'new'}) {
		push @dir_order, 'new';
		$dir_map{'new'} = '00';
		
		if (exists $dump_root_hash{'00'}) {
			push @dir_order, '00';
			$dir_map{'00'} = '01';
			
			if ($max_old) {
				foreach my $idx (1 .. $max_old) {
					my $name = sprintf("%02d", $idx);
					last unless exists $dump_root_hash{$name};
					push @dir_order, $name;
					$dir_map{$name} = sprintf("%02d", $idx+1);
				} # foreach archival iteration
			} # if we're keeping archival copies
		} # if there is a current directory as well
	} # if there is a new directory
	
	if (@dir_order) {
		push @LOG, "The following dump dirs will be renamed: "
			. join(", ", @dir_order) . ".\n";
	} # if there are directories to rename
	else {
		push @LOG, "No dump dirs will be renamed.\n";
	} # else note there are none

	# rotate names of the dump dirs we want to keep
	foreach my $old_dname (reverse @dir_order) {
		my $new_dname = $dir_map{$old_dname};
		next if $old_dname eq $new_dname;
		next unless (-d "$dump_root/$old_dname");
		next if (-f "$dump_root/$new_dname");
		push @LOG, "Renaming dump dir $old_dname/ to $new_dname/ in $dump_root/ ...\n";
		File::Copy::move("$dump_root/$old_dname", "$dump_root/$new_dname") 
			or $self->error("Cannot rename $old_dname/ to $new_dname/\n");
	} # for each directory to rename

	# delete oldest dump dir if it exceeds the specified number of copies
	# can't this just be made a delete above if last or such?
	my $oldest_dir = $dump_root . '/' . sprintf("%02d", $max_old+1);
	if (-d $oldest_dir) {
		push @LOG, "Deleting $oldest_dir/ ...\n";
		eval { File::Path::rmtree($oldest_dir) };
		$self->error("Cannot delete $oldest_dir/ - $@\n") if ($@);
		$self->error("$oldest_dir/ deleted, but still exists!\n") if (-d $oldest_dir);
	} # delete past-max oldest archive

} # end _rotate_dump_dirs


=head1 _tidy_dump_dirs()

	The dump directories contain output from both the full, weekly mysql
	dump as well as the incremental binary update logs that follow the
	dump (possibly multiple binlogs per day).  Sometimes a user might
	delete a directory between backup runs (particularly if it has bad
	dumps).

	This function is intended to be run before backups start.  It will
	Attempt to make directory names to conform to convention:

	  [dump_dir]/00/  - most recent dump
	  [dump_dir]/01/   - next most recent
	  ...
	  [dump_dir]/NN/	- oldest

	If there are missing directories, _tidy_dump_dirs will create a
	directory to take its place, such that 00/ should always exist
	and there should be no gaps in the numbering of old directories.  In
	other words, N+1 should be the total number of directories in [dump_dir].

	If there are no gaps to begin with, _tidy_dump_dirs does not rename
	anything.

	This function will also delete any xx directories that exceed the
	[dump_copies] config variable.

	It will never touch [dump_dir]/new/.  It will never modify the contents
	of any of these subdirectories (unless its deleting the whole subdir).

	It will create [dump_dir] and [dump_dir]/00/ if they do not exist.

=cut

# is this routine doing redundant work with above?!?!
sub _tidy_dump_dirs {
	my $self = shift;
	
	my $dump_copies = $self->backup_params('DUMP_COPIES');
	my $dump_root = $self->backup_params('DUMP_DIR');
	$self->_test_create_dirs($dump_root);
	
	# grab list of files/dirs within dump_root - @dump_root_files
	# create hash of dirs we care about w/in dump_root - %dump_root_hash
	opendir(DIR, $dump_root) 
		or $self->error("Cannot get listing of files in $dump_root\n");
	my @dump_dirs = grep {-d "$dump_root/$_"} readdir(DIR);
	closedir(DIR);
	my %dump_root_hash;
	foreach my $dump_dir (@dump_dirs) {
		$dump_root_hash{$dump_dir} = 1 if $dump_dir =~ /^\d+$/;
		$dump_root_hash{$dump_dir} = 1 if $dump_dir =~ /^00$/;
	} # for each dump directory
	# (the next line requires that [dump_copies] is <= 100) # huh?! why? - spq
	my @dump_root_dirs = sort keys %dump_root_hash;
	
	# prepare instructions on how to rename directories, and in what order
	# also prep instructions on which directories to delete (in case user 
	# has reduced [dump_copies] in the config file since the last time
	# this script was run)
	my %dir_map;
	my @ren_queue;
	my @del_queue;
	my $idx=0;
	foreach my $dir (@dump_root_dirs) {
		if ($idx < $dump_copies) {
			$dir_map{$dir} = sprintf("%02d", $idx);
			push @ren_queue, $dir 
				unless ($dir eq $dir_map{$dir}) or ($dir eq '00');
		} # if dump dir < max copies
		else {
			push @del_queue, $dir;
		} # else prepare delete queue
		$idx++;
	} # foreach dump dir
	
	$dir_map{$dump_root_dirs[0]} = '00' if @dump_root_dirs;
	push(@LOG, "The following dump dirs will be renamed: "
		. join(", ", @ren_queue) . ".\n") if @ren_queue;
	push(@LOG, "The following dump dirs will be deleted: "
		. join(", ", @del_queue) . ".\n") if @del_queue;
	push(@LOG, "Dump dirs look good, not much to tidy up\n")
		if not @ren_queue and not @del_queue;
	
	# shuffle names of the dump dirs
	foreach my $old_dname (@ren_queue) {
		my $new_dname = $dir_map{$old_dname};
		next if $old_dname eq $new_dname;
		next unless (-d "$dump_root/$old_dname");
		next if (-f "$dump_root/$new_dname");
		push @LOG, "Renaming dump dir $old_dname/ to $new_dname/ in "
			. "$dump_root/ ...\n";
		File::Copy::move("$dump_root/$old_dname", "$dump_root/$new_dname") 
			or $self->error("Cannot rename $old_dname/ to $new_dname/\n");
	} # foreach old name?
	
	# delete excess dump dirs
	foreach my $dname (@del_queue) {
		$dname = "$dump_root/$dname";
		push @LOG, "Deleting $dname/ (exceeds dump_copies=$dump_copies) ...\n";
		eval { File::Path::rmtree($dname) };
		$self->error("Cannot delete $dname/ - $@\n") if ($@);
		$self->error("$dname/ deleted, but still exists!\n") if (-d $dname);
	} # for each excess dir to delete
	
	# if not @dump_root_files, create a 00/ dir
	$self->_test_create_dirs("$dump_root/00");
	
} # end _tidy_dump_dirs


=head1 error()

	Logs all the errors so far to a log file then 
	sends an email and exits.

=cut

sub error {
	my $self = shift;
	my $message = shift;
	push(@LOG, $message);
	$self->log_messages();
	$self->send_email_notification();
	exit 1;
}


=head1 send_email_notification()

	Sends the data from the 00 run of the program 
	which gets stored in the log file by email. The exact 
	behaviour for this subroutine is controlled by the 
	varibles in [mail-setup] section in the config file

=cut

sub send_email_notification {
	my $self = shift;
#	warn "self? $self";
	
	# Email notifications can be turned off although this
	# is usually not a good idea
	my $notify = $self->mail_setup('mail_notification');
	return unless $notify =~ /yes/i;
	
	# send LOG by mail
	# get the varibles below from a config file
	my $hostname = $self->db_connect('HOSTNAME');
	my $subject  = "MySQL dump log from $hostname at " . localtime; 
	
	my $to = join(', ', @{$self->mail_setup('mail_to')});
	my $cc = join(', ', @{$self->mail_setup('mail_cc')});
	
	sendmail(To  => $to,
		 CC      => $cc,
		 Subject => $subject,
		 From    => $self->mail_setup('mail_from'),
		 Message => (join '', @LOG),
		 Server  => $self->mail_setup('mail_server'),
		 delay   => 1,
		 retries => 3 ); 

} # end send_email_notification()


1;


=head1 DESCRIPTION

Manages rotation of mysql database logs and database backups. Reads information
on which databases to back up on what days fo the week from the configuration
file. If no file is specified, it will look for one at /etc/mysql-backup.conf.
If no configuration file is found program will exit with a failure.

This program assumes a MySQL 4.0.x server that is at least 4.0.10.
It will likely work with current 1.23.xx server, but that has not been tested.
Please let the maintainers know if you use this tool succesfully with other
versions of MySQL or Perl so we can note what systems it works with.

The expected usage of this program is for it to be run daily by a cron job,
at a time of day convienient to have the backups occur. This program uses the
administrative tools provided with MySQL (mysqladmin and mysqldump) as well
as gzip for compression of backups.

Every time this program is run it will flush the MySQL logs. The binary update
log will be moved into /path/to/dump/dir/00. Error log and slow query log files
are rotated only if they exceeded the size limit specified in the confguration
file.

If it is run on a day when full database backups are specified, then
all databases specified in the config file are dumped and
written to the directory specified in dump_dir variable in the config
file.  If there are no problems with this operation, previous full backups
from dump_dir/00 are moved to directory dump_dir/01 and all the
files in dump_dir/01 (full database backups and log files) are deleted
from it or moved to dump_dir/02 etc. to the archival depth specified in the
config file. This way there always [dump_copies] full database backups - 
one in 00/ and [dump_copies]-1 in the xx directories.

Detailed information about the different configuration parameters
can be found in the comments in the configuration file

log-slow-queries
log-long-format
log-bin

=head1 OPTIONS


=over 4

=item B<logfile>

Filename for logging backup proceedure. Overrides conf file.

=item B<add_databases>

Additional databases to back up. These will be backed up I<in addation> to any
databases specified in the conf file. B<Note> - this adds databases to the list
of those to be backed up. If the program is being run on a day when database
backups are not scheduled, the extra databases specified will B<not> be backed
up.

=item B<backup>

If present this option forces full database backups to be done, even if not
normally scheduled.

=item B<help>

Outputs this help file.

=item B<d>

** NOT IMPLIMENTED **

Turn on debugging. Optionally takes a filename to store debugging and any error
messages to.

=item B<v>

** NOT IMPLIMENTED **

Increases debugging vebosity. If three or more v's provided (-v -v -v)
than program will exit on warnings.

=back

=head1 TO DO

Impliment debugging output options.

=head1 HISTORY

=over 8

=item 0.8

First functional release.

=back



=head1 SEE ALSO

mysql-backup.conf

=head1 AUTHOR

Sean P. Quinlan, E<lt>sean@quinlan.orgE<gt>

Original version by Stefan Dragnev, E<lt>dragnev@molbio.mgh.harvard.eduE<gt>
with contributions from Norbert Kremer, E<lt>kremer@molbio.mgh.harvard.edu<gt>
and Danny Park, E<lt>park@molbio.mgh.harvard.edu.<gt>

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2004 by Sean P. Quinlan & Stefan Dragnev

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself, either Perl version 5.8.3 or,
at your option, any later version of Perl 5 you may have available.

=cut

